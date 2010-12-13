
require 'mechanize'
require 'sqlite3'

#
# if the directory is thought as a tree, each person is on one and only one leaf node so there's no
# need to check for duplicates
#

class Store
  def initialize
    @db = SQLite3::Database.new('facebook.db')
  end

  def reset_status
    @db.execute("UPDATE queue SET status=NULL WHERE status=0")
  end

  def next
    result = @db.execute('SELECT id, url FROM queue WHERE STATUS IS NULL ORDER BY RANDOM() LIMIT 1')
    result.first.tap do |r|
      @db.execute("UPDATE queue SET status=0 WHERE id=#{r.first}")
    end
  end

  def update(id, nodes, leaves)
    people = queue = 0

    @db.transaction do |db|
      unless leaves.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO people (url, name) VALUES(?,?)')
        leaves.each do |leaf|
          stmt.execute(leaf[:url], leaf[:title])
          people += @db.changes
        end
      end

      unless nodes.empty?
        stmt = @db.prepare('INSERT OR IGNORE INTO queue (url) VALUES(?)')
        nodes.each do |node|
          stmt.execute(node[:url])
          queue += @db.changes
        end
      end

      finish(id)
    end

    [people, queue]
  end

  def finish(id)
    @db.execute("UPDATE queue SET status=1 WHERE id=#{id}") unless id.nil?
  end

  def queue_size
    @db.execute('SELECT COUNT(1) FROM queue WHERE status IS NULL').first.first
  end

  def people_size
    @db.execute('SELECT COUNT(1) FROM people').first.first
  end
attr_reader :db

end

def download(agent, url)
  retries = 0
  page = nil
  begin
    print url
    return agent.get(url)
  rescue Errno::ECONNREFUSED, Timeout::Error, Net::HTTPInternalServerError
    retries += 1
    puts " Error: #{$!.message}"
    if retries > 3
      puts "Too many retries"
      exit
    end
    sleep 15
    retry
  end
end

def parse(page)
  links = page.links.select { |link| link.href =~ %r{/people/.} }
  links.map! { |link| { :url => link.href, :title => link.text } }

  links.partition { |link| link[:url] =~ %r{/directory/} }
end


$db = Store.new
$db.reset_status

stmt = $db.db.prepare('UPDATE people SET digest=? WHERE id=?')

n = i = 0
results = $db.db.query('SELECT id,url,name FROM people')
file = nil
results.each { |x|
  file ||= File.new("people-#{n}.csv", 'w')
  file.puts x[1,2].join("\t")
  i += 1
  if i % 1000000 == 0
    n += 1
    i = 0
    file.close
    file = nil
  end
}
file.close unless i == 0
exit


n = 0
while true do
  result = $db.next
  if result.nil?
    puts "No more URLs"
    break
  end

  id = result.first
  url = result.last
    
  agent = Mechanize.new
  agent.user_agent_alias = 'Windows IE 7'

  page = download(agent, url)
  nodes, leaves = parse(page)

  people, queue = $db.update(id, nodes, leaves)

  buf = "\t"
  buf += "#{queue}/" if queue != nodes.size
  buf += "#{nodes.size}\t"
  buf += "#{people}/" if people != leaves.size
  buf += "#{leaves.size}"
  puts buf

  n += 1
  if n % 50 == 0
    queue = $db.queue_size
    people = $db.people_size
    puts "queue: #{queue}, people: #{people}"
  end
end
