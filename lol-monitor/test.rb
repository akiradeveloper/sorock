N=3
ND=[]
(0...N).each do |i|
  j = 50+i
  ND << "http://localhost:500#{j}"
end

pidlist = []

ND.each do |nd|
  pidlist << Process.spawn("kvs-server #{nd}")
end
sleep(5)

MASTER=ND[0]
(0...N).each do |i|
  `lol-admin #{MASTER} add-server #{ND[i]}`
  sleep(2)
end

2.times do
  pidlist << Process.fork do
    loop do
      `kvs-client #{MASTER} set k v`
    end
  end
end

system "cargo run --bin lol-monitor #{MASTER}"

pidlist.each do |pid|
  Process.detach(pid)
  Process.kill(9, pid)
end