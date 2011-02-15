
$LOAD_PATH << File.join(File.dirname(__FILE__))

require 'mechanize'
require 'fileutils'
require 'sqlite3'
require 'store'
require 'download'

$stdout.sync = true
Thread.abort_on_exception = true

#
# if the directory is thought as a tree, each person is on one and only one
# leaf node so there's no need to check for duplicates
#

class Harvester
  def initialize(db)
    @downloader = Downloader.new
    @db = db
  end

  def run
    n = 0
    while true do
      result = @db.next
      if result.nil?
        puts "No more URLs"
        break
      end

      id = result.first
      url = result.last
        
      @downloader.get(url) do |nodes, leaves, url|
        queue, people = @db.update(id, nodes, leaves)
        print_stats(url, queue, nodes, people, leaves)
        n += 1
        if n > 50
          n = 0
          puts "queue: #{@db.queue_size}"
        end
      end
    end
    @downloader.wait_workers
  end

  protected

  def print_stats(url, queue, nodes, people, leaves)
    buf = "#{url}\t"
    buf += "#{queue}/" if queue != nodes.size
    buf += "#{nodes.size}\t"
    buf += "#{people}/" if people != leaves.size
    buf += "#{leaves.size}"
    puts buf
  end
end


$db = Store.new
$db.reset_status

Harvester.new($db).run
