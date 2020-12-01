ND=[]
(0...10).each { |i|
    offset = 50+i
    ND << "http://localhost:500#{offset}"
}
# FIXME:
# kill the kvs-server processes after this script is done.
ND.each { |nd|
    Thread.fork {
        `kvs-server #{nd}`
    }
}
sleep(5)

MASTER=ND[0]
(0...10).each { |i|
    `lol-admin #{MASTER} add-server #{ND[i]}`
    sleep(2)
}

5.times {
    Thread.fork {
        loop {
            `kvs-client #{MASTER} set k v`
        }
    }
}

system "cargo run --bin lol-monitor #{MASTER}"