require 'bundler/setup'

require 'csv'

require 'active_support/core_ext/integer/inflections'
require 'active_support/time'
require 'active_model'
require 'fog'
require 'multi_xml'
require 'nokogiri'
require 'pupa'
require 'redis-store'
require 'hiredis'

class Pupa::Membership
  attr_reader :person, :post
  dump :person, :post
  foreign_object :person, :post

  def person=(person)
    @person = {_type: 'pupa/person'}.merge(symbolize_keys(person))
  end

  def post=(post)
    @post = {_type: 'pupa/post'}.merge(symbolize_keys(post))
  end
end

class Pupa::Person
  include ActiveModel::Validations
  validates_inclusion_of :gender, in: %w(male female)
end

class Pupa::Post
  include Pupa::Concerns::Identifiable
  attr_accessor :area, :position
  dump :area, :position

  def fingerprint
    super.slice(:label, :organization_id, :end_date, :position) # adds position
  end
end

# Stores data downloads on disk.
#
# @see ActiveSupport::Cache::FileStore
class DownloadStore < Pupa::Processor::DocumentStore::FileStore
  # Returns all file names in the storage directory.
  #
  # @return [Array<String>] all keys in the store
  def entries
    Dir.chdir(@output_dir) do
      Dir['*']
    end
  end

  # Returns the contents of the file with the given name.
  #
  # @param [String] name a key
  # @return [Hash] the value of the given key
  def read(name)
    File.open(path(name)) do |f|
      f.read
    end
  end

  # Writes the value to a file with the given name.
  #
  # @param [String] name a key
  # @param [Hash,String] value a value
  def write(name, value)
    File.open(path(name), 'w') do |f|
      f.write(value)
    end
  end

  # Deletes all files in the storage directory.
  def clear
    Dir[File.join(@output_dir, '*')].each do |path|
      File.delete(path)
    end
  end

  # Returns the byte size of the file.
  #
  # @param [String] name a key
  # @return [Integer] the file size in bytes
  def size(name)
    File.size(path(name))
  end
end

# Stores data downloads on AWS.
#
# @see ActiveSupport::Cache::FileStore
class AWSStore < Pupa::Processor::DocumentStore::FileStore
  # @param [String] output_dir the directory in which to download data
  # @param [String] bucket the AWS bucket in which to store data
  # @param [String] aws_access_key_id an AWS access key ID
  # @param [String] aws_secret_access_key an AWS secret access key
  # @see http://fog.io/storage/#using-amazon-s3-and-fog
  def initialize(output_dir, bucket, aws_access_key_id, aws_secret_access_key)
    @output_dir = output_dir
    @connection = Fog::Storage.new(provider: 'AWS', aws_access_key_id: aws_access_key_id, aws_secret_access_key: aws_secret_access_key, path_style: true)
    @bucket = @connection.directories.get(bucket, prefix: output_dir)
  end

  # Returns whether a file with the given name exists.
  #
  # @param [String] name a key
  # @return [Boolean] whether the store contains an entry for the given key
  # @see http://fog.io/storage/#checking-if-a-file-already-exists
  def exist?(name)
    !!@bucket.files.head(path(name))
  end

  # Returns all file names in the storage directory.
  #
  # @return [Array<String>] all keys in the store
  def entries
    @bucket.files.map(&:key)
  end

  # Returns the contents of the file with the given name.
  #
  # @param [String] name a key
  # @return [Hash] the value of the given key
  # @see http://fog.io/storage/#backing-up-your-files
  def read(name)
    @bucket.files.get(path(name)).body
  end

  # Writes the value to a file with the given name.
  #
  # @param [String] name a key
  # @param [Hash,String] value a value
  # @see http://fog.io/storage/#using-amazon-s3-and-fog
  def write(name, value)
    @bucket.files.new(key: path(name), body: value, public: true).save
  end

  # Delete a file with the given name.
  #
  # @param [String] name a key
  # @see http://fog.io/storage/#cleaning-up
  def delete(name)
    @bucket.files.get(path(name)).destroy
  end

  # Deletes all files in the storage directory.
  def clear
    @connection.delete_multiple_objects(@bucket.key, entries)
  end
end
