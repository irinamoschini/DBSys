JDKPATH = /usr/lib/jvm/java-11-openjdk-amd64
LIBPATH = ./..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test buffer manager

BMTest2:BMTest2.java
	$(JAVAC) TestDriver.java BMTest2.java

bmtest2: BMTest2
	$(JAVA) tests.BMTest2

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest

# test join

JoinTest2:JoinTest2.java
	$(JAVAC) TestDriver.java JoinTest2.java

jointest2: JoinTest2
	$(JAVA) tests.JoinTest2



