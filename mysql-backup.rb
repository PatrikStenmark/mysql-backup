#!/usr/bin/env ruby
require 'yaml'
require 'rubygems'
CONFIG = YAML::load(File.read(ARGV[0]))

if CONFIG[:s3]
  require 'aws'
end

unless File.exists?(CONFIG[:mysql_config])
  puts "No config file: #{CONFIG[:mysql_config]}"
  exit 1
end

if File.stat(CONFIG[:mysql_config]).gid != 0 && (File.stat(CONFIG[:mysql_config]).mode & 0077) != 0
  puts "Noooo, #{CONFIG[:mysql_config]} shall not be readable by any other than root"
  raise SystemExit
end

mysqldump = CONFIG[:mysqldump] || "mysqldump"


def run(cmd)
  debug("Running #{cmd}") 
  system(cmd)
end

def debug(str)
  if CONFIG[:verbose]
    puts str
  end
end

unless File.exists?(CONFIG[:target_dir])
  Dir.mkdir(CONFIG[:target_dir], 0700)
end

CONFIG[:databases].each do |database|
  target_dir = File.join(CONFIG[:target_dir], database)
  unless File.exists?(target_dir)
    Dir.mkdir(target_dir, 0700)
  end
  debug("Cleaning up old backups for #{database}")
  
  backups = Dir[File.join(target_dir, "*.bz2")].sort.reverse
  debug("backups = #{backups.inspect}")
  debug("keep = #{CONFIG[:keep_backups]}")
  backups_to_delete = backups.slice!(CONFIG[:keep_backups].to_i, backups.size) || []
  debug("to delete = #{backups_to_delete.inspect}")
  
  backups_to_delete.each do |backup_to_delete|
    debug("Would delete #{backup_to_delete}")
    File.delete(backup_to_delete)
  end
  filename = File.join(target_dir, "#{database}-#{Time.now.strftime("%Y%m%d%s")}.bz2")
  debug("filename = #{filename}")
  run("#{mysqldump} --defaults-file=#{CONFIG[:mysql_config]} #{database} | bzip2 > #{filename}")
  #run("touch #{filename}")
  
  if CONFIG[:s3]
    raise "S3 needs :s3_bucket, :s3_access_key_id and :s3_secret_access_key" unless CONFIG[:s3_secret_access_key] && CONFIG[:s3_access_key_id]
    s3 = Aws::S3.new(
      CONFIG[:s3_access_key_id],
      CONFIG[:s3_secret_access_key]
    )
    
    s3_key = File.basename(filename)
    debug("Storing #{s3_key} in S3")
    bucket = s3.bucket(CONFIG[:s3_bucket])
    bucket.put(s3_key, open(filename))
    debug("Done storing #{filename} in S3")
    
    debug("Deleting local #{filename}")
    File.delete(filename)
  end
end
