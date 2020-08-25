require 'digest'
require 'aws-sdk-s3'
require 'logger'
require 'ruby-progressbar'
require 'parallel'
require 'friendly_numbers'

class S3Backer
  DEEP_ARCHIVE = 'DEEP_ARCHIVE'.freeze

  attr_accessor :client, :resource

  def initialize(region:, credentials: nil, logger: nil)
    if credentials
      @client = Aws::S3::Client.new(:region => region, :credentials => credentials)
    else
      @client = Aws::S3::Client.new(:region => region)
    end
    @resource = Aws::S3::Resource.new(client: @client)
    @log = logger || Logger.new(STDOUT)
  end

  # the file "#{backup_directory}/file.txt" will be uploaded to bucket::"#{key_prefix}/file.txt"
  def start_backup(backup_directory:, bucket:, key_prefix:)
    @log.info "Preparing backing up '#{backup_directory}' to '#{bucket}::key_prefix'"
    all_file_names = all_files(backup_directory)
    @log.info "Found #{all_file_names.size} files"
    @log.info "Computing total size..."

    file_size_map = Parallel.map(all_file_names, progress: "Computing total size") do |file_name|
      path = File.join backup_directory, file_name
      file = File.open(path)
      [file_name, file.size]
    end.to_h
    total_size = file_size_map.values.inject(0) { |sum,x| sum + x }
    @log.info "Total Size: #{total_size}"
    failed_files = []
    progressbar = ProgressBar.create(title: "Backing up",
                                     total: total_size,
                                     format: "%t %a %e %P%")

    update_progress_and_collect_failure = -> (file_name, i, success) {
      size = file_size_map[file_name]
      progressbar.progress += size
      unless success
        failed_files << file_name
      end
    }

    Parallel.each(all_file_names, finish: update_progress_and_collect_failure ) do |file_name|
      path = File.join backup_directory, file_name
      key = File.join key_prefix, file_name

      file = File.open(path)
      object = @resource.bucket(bucket).object(key)

      checksum = Digest::MD5.file file

      if object.exists?
        if object.etag == checksum.to_s.inspect
          @log.info "Object '#{file_name}' already exist and is same as local file"
          true
          next
        else
          @log.info "Object '#{file_name}' already exist but is different from the local file"
        end
      end
      @log.info "Backing up #{file_name} (#{FriendlyNumbers.number_to_human_size(file_size_map[file_name])})"
      begin
        object.put(body: file, storage_class: DEEP_ARCHIVE, content_md5: checksum.base64digest)
        true
      rescue Aws::S3::Errors::EntityTooLarge => e
        @log.error "File '#{file_name}' is too large to be uploaded in a single request, skipping"
        false
      rescue Aws::S3::Errors::ServiceError => e
        @log.error "Failed to upload file '#{file_name}' due to error '#{e}'"
        @log.error e
        false
      end
    end
    @log.info "Done"
  end

  def all_files(directory)
    original_dir = Dir.pwd
    Dir.chdir directory

    all_descendants_pattern = File.join("**", "*")

    file_names = Dir.glob(all_descendants_pattern).select do |file_name|
      File.file?(file_name)
    end
    Dir.chdir original_dir
    file_names
  end
end
