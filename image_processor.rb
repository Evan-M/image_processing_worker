require 'json'
require 'mysql2'
require 'active_record'

require 'open-uri'
require 'uri'

require 'fog'
require 'subexec'
require 'mini_magick'

class MediaAsset < ActiveRecord::Base
  has_one :offer, foreign_key: :poster_asset_id, inverse_of: :poster_asset
end

class Offer < ActiveRecord::Base
  self.primary_key = "offer_number"
  belongs_to :poster_asset, class_name: "MediaAsset", inverse_of: :offer
end

def original(image, h)
  original_width, original_height = image[:width], image[:height]
  image.combine_options do |c|
    c.strip
    c.resize "#{original_width}x#{original_height}"
  end
  image
end

def resize(image, h)
  original_width, original_height = image[:width], image[:height]
  h['width'] ||= original_width
  h['height'] ||= original_height
  image.resize "#{h['width']}x#{h['height']}"
  image
end

def thumbnail(image, h)
  image.combine_options do |c|
    c.thumbnail "#{h['width']}x#{h['height']}"
    c.background 'white'
    c.extent "#{h['width']}x#{h['height']}"
    c.gravity "center"
  end
  image
end

def sketch(image, h)
  image.combine_options do |c|
    c.edge "1"
    c.negate
    c.normalize
    c.colorspace "Gray"
    c.blur "0x.5"
  end
  image
end

def offerize(image, h)
  h = {
    'brightness' => '115',
    'saturation' => '175',
    'hue'        => '100',
    'gamma'      => '1.125',
    'width'      => '350',
    'height'     => '350'
  }.merge(h)

  image.combine_options do |c|
    c.modulate "#{h['brightness']},#{h['saturation']},#{h['hue']}"
    c.gamma "#{h['gamma']}"
    c.gravity "center"
    c.resize "#{h['width']}x#{h['height']}"
  end
  image
end

def normalize(image, h)
  image.normalize
  image
end

def charcoal(image, h)
  image.charcoal '1'
  image
end

def level(image, h)
  image.level " #{h['black_point']},#{h['white_point']},#{h['gamma']}"
  image
end

def tile(h)
  file_list=[]
  image = MiniMagick::Image.open(filename)
  original_width, original_height = image[:width], image[:height]
  slice_height = original_height / h['num_tiles_height']
  slice_width = original_width / h['num_tiles_width']
  h['num_tiles_width'].times do |slice_w|
    file_list[slice_w]=[]
    h['num_tiles_height'].times do |slice_h|
      output_filename = "filename_#{slice_h}_#{slice_w}.jpg"
      image = MiniMagick::Image.open(filename)
      image.crop "#{slice_width}x#{slice_height}+#{slice_w*slice_width}+#{slice_h*slice_height}"
      image.write output_filename
      file_list[slice_w][slice_h] = output_filename
    end
  end
  file_list
end

def merge_images(col_num, row_num, file_list)
  output_filename = "merged_file.jpg"
  ilg = Magick::ImageList.new
  col_num.times do |col|
    il = Magick::ImageList.new
    row_num.times do |row|
      il.push(Magick::Image.read(file_list[col][row]).first)
    end
    ilg.push(il.append(true))
    ilg.append(false).write(output_filename)
  end
  output_filename
end

def s3
  Fog::Storage.new({
    provider:                 'AWS',
    aws_access_key_id:        params['aws']['access_key'],
    aws_secret_access_key:    params['aws']['secret_key']
  })
end

def get_bucket(bucket_name)
  s3.directories.create(key: bucket_name, public: true)
end

def create_public_file_on_bucket(bucket, path, filepath)
  bucket.files.create(
    key: "#{path}#{filepath}",
    body: File.open(filepath),
    public: true
  )
end

def upload_file(filename, path=nil)
  unless params['disable_network']

    # Check that the offer to attach the image to exists before doing anything
    offer = Offer.find( params['offer_id'] )
    unless offer
      puts "No offer to attach image to"
    end

    bucket_name = params['aws']['s3_bucket_name']
    path = path && (!path.end_with?('/') && "#{path}/" || "#{path}") || ""
    files = [filename].flatten
    files.each do |filepath|
      puts "Uploading the file #{filepath} to s3://#{bucket_name}/#{path}"

      stored_file = create_public_file_on_bucket( get_bucket(bucket_name), path, filepath )

      if stored_file
        puts "Uploading successful."
        puts "\nYou can view the file here on s3: ", stored_file.public_url

        asset_attributes = {name: "#{filepath}", uri: "#{stored_file.public_url}"}
        puts "Saving asset record to database associated with offer ##{params['offer_id']} with #{asset_attributes.inspect}"
        if offer.poster_asset
          offer.poster_asset.update_attributes! asset_attributes
        else
          offer.create_poster_asset asset_attributes
        end
      else
        puts "Error uploading to s3."
      end
      puts "-"*60
    end
  end
end

def filename
  File.basename(params['source_image_url']).split('?')[0]
end

def download_source_image()
  puts "Downloading source image: #{filename}"
  unless params['disable_network']
    File.open(filename, 'wb') do |fout|
      open(params['source_image_url']) do |fin|
        IO.copy_stream(fin, fout)
      end
    end
  end
  filename
end

def delete_source_image()
  puts "Deleting source image: #{filename}"

  bucket_name = params['aws']['s3_bucket_name']
  bucket = get_bucket( bucket_name )

  # Utilizing 'new' won't actually create an object; just a local representation (ie no api calls)
  file = bucket.files.new(key: params['source_image_keypath'])
  file.destroy
end

def get_config()
  config = {}
  ARGV.each_with_index do |arg, i|
    if arg == "-config"
      config = JSON.parse(IO.read(ARGV[i+1]))
    end
  end
  config
end

def setup_database(db_params)
  return unless db_params
  # estabilsh database connection
  ActiveRecord::Base.establish_connection( db_params )
end

puts "Worker started"
p params

puts "Fetching config"
config = get_config

puts "Connecting to database"
setup_database( config['database_params'] )

puts "Downloading source image"
filename = download_source_image

params['operations'].each do |op|
  puts "\n\nPerforming #{op[:op]} with #{op.inspect}"
  output_path = op['destination_path']
  output_filename = op['destination']
  image = MiniMagick::Image.open(filename)
  image = self.send(op[:op], image, {}.merge(op))
  image.strip unless op[:strip] === false
  image.format op['format'] if op['format']
  image.quality op['quality'] if op['quality'] unless op[:op] == "original"
  image.write output_filename
  upload_file output_filename, output_path
end

puts "Cleaning up."
delete_source_image

puts "Worker finished"
