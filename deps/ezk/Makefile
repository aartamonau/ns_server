ZK_VERSION=3.3.3
ZK_URL=https://archive.apache.org/dist/zookeeper/zookeeper-3.3.3/zookeeper-3.3.3.tar.gz
# ZK_URL=http://hudson/job/zookeeper-mirror/7/artifact/zookeeper-3.3.3.tar.gz
ZK_ARCHIVE=zookeeper-${ZK_VERSION}.tar.gz
ZK_DIR=zookeeper-${ZK_VERSION}


compile:

conf_local:
	cp -f ./configs/local_ezk.app.src ./src/ezk.app.src

conf_extern:
	cp -f ./configs/extern_ezk.app.src ./src/ezk.app.src

## Hudson's continuous integration rule
ci: clean conf_local compile test

## compile test release

erlstart:
	erl -pa "/home/marco/ezk/ebin/"

dialyze: compile
	dialyzer ebin/*.beam --build_plt

common_move:
	mkdir -p ct_log
	mv ct_run* ct_log/

compile:
	./rebar compile

test:	compile zk_start testen zk_stop

testen:
	sleep 5
	./rebar ct
	sleep 5



zk_start: zk_config
	ZOOCFGDIR=./zookeeper/zoo1/conf ./zookeeper/zoo1/bin/zkServer.sh start zoo.cfg
	ZOOCFGDIR=./zookeeper/zoo2/conf ./zookeeper/zoo1/bin/zkServer.sh start zoo.cfg
	ZOOCFGDIR=./zookeeper/zoo3/conf ./zookeeper/zoo1/bin/zkServer.sh start zoo.cfg

zk_start_s: zk_config
	ZOOCFGDIR=./zookeeper/zoosingle/conf ./zookeeper/zoosingle/bin/zkServer.sh start zoo.cfg

zk_stop_s:
	ZOOCFGDIR=./zookeeper/zoosingle/conf ./zookeeper/zoosingle/bin/zkServer.sh stop

zk_stop:
	ZOOCFGDIR=./zookeeper/zoo1/conf ./zookeeper/zoo1/bin/zkServer.sh stop
	ZOOCFGDIR=./zookeeper/zoo2/conf ./zookeeper/zoo2/bin/zkServer.sh stop
	ZOOCFGDIR=./zookeeper/zoo3/conf ./zookeeper/zoo3/bin/zkServer.sh stop

zk_config: zk_clone
	mkdir -p ./zookeeper/data/zoo1
	mkdir -p ./zookeeper/data/zoo2
	mkdir -p ./zookeeper/data/zoo3
	mkdir -p ./zookeeper/data/zoosingle
	echo 1 > ./zookeeper/data/zoo1/myid
	echo 2 > ./zookeeper/data/zoo2/myid
	echo 3 > ./zookeeper/data/zoo3/myid
	cp ./configs/zoo1.cfg ./zookeeper/zoo1/conf/zoo.cfg
	cp ./configs/zoo2.cfg ./zookeeper/zoo2/conf/zoo.cfg
	cp ./configs/zoo3.cfg ./zookeeper/zoo3/conf/zoo.cfg
	cp ./configs/zoosingle.cfg ./zookeeper/zoosingle/conf/zoo.cfg

zk_clone: ${ZK_DIR}
	mkdir -p ./zookeeper/zoo1
	mkdir -p ./zookeeper/zoo2
	mkdir -p ./zookeeper/zoo3
	mkdir -p ./zookeeper/zoosingle
	cp -r -f ${ZK_DIR}/* ./zookeeper/zoo1
	cp -r -f ${ZK_DIR}/* ./zookeeper/zoo2
	cp -r -f ${ZK_DIR}/* ./zookeeper/zoo3
	cp -r -f ${ZK_DIR}/* ./zookeeper/zoosingle

${ZK_DIR}: ${ZK_ARCHIVE}
	tar xzf $<

${ZK_ARCHIVE}:
	wget ${ZK_URL} -q

clean:
	rm -rf ${ZK_DIR}


.PHONY: clean zk_start zk_config compile test testen ci
