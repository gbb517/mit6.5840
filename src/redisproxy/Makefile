-include ../../Global.mk

ROOTDIR = ../..
LIBS    := $(LIBS) -lshardkvraft -lraft -lkvraft -lkvraft-rpc -lmuduo_net -lmuduo_base

REDISPROXY_SRC = RedisProxyServer.cpp
REDISPROXY_OBJ = $(addprefix $(OBJECTS_DIR)/, $(REDISPROXY_SRC:cpp=o))
OBJECTS         = $(REDISPROXY_OBJ)

DEPENDENCIES = $(LIB_DIR)/libshardkvraft.a $(LIB_DIR)/libraft.a $(LIB_DIR)/libkvraft.a $(LIB_DIR)/libkvraft-rpc.a

all: $(BIN)/redisproxy

$(BIN)/redisproxy: main.cpp $(OBJECTS) $(DEPENDENCIES)
	$(CC) $(OBJECTS) main.cpp $(CPPFLAGS) $(LIBS) $(LDFLAGS) $(INC) -I ../../include -o $@

-include $(OBJECTS:.o=.d)

$(OBJECTS_DIR)/%.d: %.cpp
	@set -e; \
	mkdir -p $(OBJECTS_DIR); \
	rm -f $@; \
	$(CC) $(CPPFLAGS) $(INC) -I ../../include -MM -MT $(@:.d=.o) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

$(OBJECTS): %.o:
	$(CC) $(CPPFLAGS) $(INC) -I ../../include -c $< -o $@

.PHONY: clean
clean:
	rm -rf $(BIN)/redisproxy $(OBJECTS)
