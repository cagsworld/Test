##########################################################
# Name:
#   pb_pbl_setup.rb
# 
# Purpose:
#   1. To create separate separate subdirectories for each
#   *.pbl file contained in the specified root directory and
#   move each pbl the the newly created directory for it.
#   2. To modify the libpath directory contained i root 
#   directory so that it correctly references new pbl 
#   locations.  
#
##########################################################

require 'fileutils'

# Capture all command line args to meainingful var names
@rootdir = ARGV[0]  # path to root directory
@libfile = ARGV[1]  # name of libpath file
@applpbl = ARGV[2]  # name of the application pbl
# Convert dos path to nix path
#@rootdir = @rootdir.gsub '\\', '/' if @rootdir[1,2] == '\\'
# Default libfile if not entered
if ARGV[1] == nil
  @libfile = 'libfile.txt'
end     

#
# Main (high level) logic 
#
def main
  puts "Started"
  Dir.foreach(@rootdir) {|filename|
    if filename =~ /.*[.]pbl/i
      this_filename = File.basename(filename.downcase, '.pbl')
      unless this_filename == @applpbl.downcase
        pathname = (@rootdir + '\\' + filename).downcase
        new_dir = @rootdir + '\\' + File.basename(filename.downcase, '.pbl') + '\\'
        puts pathname
        puts new_dir
        Dir.mkdir new_dir
        FileUtils.move pathname, new_dir
      end
    end  
  }
  update_lib_path_file
  puts "Done"
end

#
# Helper classes and methods needed by main logic above begin here
#
def update_lib_path_file
  pathname = (@rootdir + '\\' + @libfile).downcase
  fw = File.new(pathname + '.new', 'w')
  f = File.new(pathname)
  f.each {|l|
    line = l.chomp.chomp(';')
    if File.basename(line.downcase, '.pbl') == @applpbl.downcase
      puts l
      fw.puts l
    else 
      puts line.gsub(File.basename(line),File.basename(line,'.pbl')) + '\\' + File.basename(line) + ';'
      fw.puts line.gsub(File.basename(line),File.basename(line,'.pbl')) + '\\' + File.basename(line) + ';'
      #puts line =~ /\\([^\\])+$/
      #puts line[/\\([^\\])+$/]
    end  
  }
end  

def clone_dir_tree
  Dir.mkdir(@rootdir + '\\mirror') unless File.exists?(@rootdir + '\\mirror')
  Dir.foreach(@rootdir) {|filename|
    if File.directory?(@rootdir + '\\' + filename)
      if not File.exists?(@rootdir + '\\mirror\\' + filename)
        puts filename
        Dir.mkdir( @rootdir + '\\mirror\\' + filename)
      end  
    end  
  }
end

def clean_dir
  puts "Started"
  Dir.foreach(@rootdir) {|filename|
    if filename =~ /.*[.]sr?/i
      puts filename
    end  
  }
  puts "Done"
end

# Excute top level logic located in main method at top of this file
main
#clone_dir_tree
clean_dir


