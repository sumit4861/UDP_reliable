# Makefile for KTP (KGP Transfer Protocol) Java Implementation
# Compatible with GNU Make on Linux and Windows (MinGW / Git Bash)

JAVAC = javac
JAVA = java
JAR = jar
JFLAGS = -g -Xlint:unchecked

SOURCES = $(wildcard *.java)
CLASSES = $(SOURCES:.java=.class)
JAR_NAME = ktp.jar

all: $(JAR_NAME)

%.class: %.java
	$(JAVAC) $(JFLAGS) $<

$(JAR_NAME): $(CLASSES)
	$(JAR) cfe $(JAR_NAME) InitKSocket *.class

initksocket: $(JAR_NAME)
	$(JAVA) -cp $(JAR_NAME) InitKSocket

user1: $(JAR_NAME)
	$(JAVA) -cp $(JAR_NAME) User1 input.txt

user2: $(JAR_NAME)
	$(JAVA) -cp $(JAR_NAME) User2 output.txt

test: $(JAR_NAME)
	$(JAVA) -cp $(JAR_NAME) EvaluateP input.txt

test-large: $(JAR_NAME)
	$(JAVA) -cp $(JAR_NAME) EvaluateP large_input.txt

clean:
	rm -f *.class $(JAR_NAME) output*.txt
