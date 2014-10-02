require 'open-uri'

require 'fog'
require 'subexec'
require 'mini_magick'

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

def upload_file(filename, path=nil)
  unless params['disable_network']
    bucket_name = params['aws']['s3_bucket_name']
    path = path && (!path.end_with?('/') && "#{path}/" || "#{path}") || ""
    files = [filename].flatten
    files.each do |filepath|
      puts "Uploading the file #{filepath} to s3://#{bucket_name}/#{path}"

      s3 = Fog::Storage.new({
        provider:                 'AWS',
        aws_access_key_id:        params['aws']['access_key'],
        aws_secret_access_key:    params['aws']['secret_key']
      })

      bucket = s3.directories.create(
        key: bucket_name,
        public: true
      )

      stored_file = bucket.files.create(
        key: "#{path}#{filepath}",
        body: File.open(filepath),
        public: true
      )

      if stored_file
        puts "Uploading successful."
        puts "\nYou can view the file here on s3: ", stored_file.public_url
      else
        puts "Error uploading to s3."
      end
      puts "-"*60
    end
  end
end

def filename
  File.basename(params['image_url'])
end

def download_image()
  puts "Downloading file: #{filename}"
  unless params['disable_network']
    File.open(filename, 'wb') do |fout|
      open(params['image_url']) do |fin|
        IO.copy_stream(fin, fout)
      end
    end
  end
  filename
end


puts "Worker started"
p params
puts "Downloading image"
filename = download_image
params['operations'].each do |op|
  puts "\n\nPerforming #{op[:op]} with #{op.inspect}"
  output_path = op['destination_path']
  output_filename = op['destination']
  image = MiniMagick::Image.open(filename)
  image = self.send(op[:op], image, {}.merge(op))
  image.format op['format'] if op['format']
  image.quality op['quality'] if op['quality'] unless op[:op] == "original"
  image.write output_filename
  upload_file output_filename, output_path
end
puts "Worker finished"
