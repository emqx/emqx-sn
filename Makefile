PROJECT = emqx_sn
PROJECT_DESCRIPTION = EMQ X MQTT-SN Gateway

DEPS = esockd clique
dep_esockd = git-emqx https://github.com/emqx/esockd v5.4.3
dep_clique = git-emqx https://github.com/emqx/clique v0.3.11

CUR_BRANCH := $(shell git branch | grep -e "^*" | cut -d' ' -f 2)
BRANCH := $(if $(filter $(CUR_BRANCH), master develop), $(CUR_BRANCH), develop)

BUILD_DEPS = emqx cuttlefish
dep_emqx = git-emqx https://github.com/emqx/emqx $(BRANCH)
dep_cuttlefish = git-emqx https://github.com/emqx/cuttlefish v2.2.1

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform}'

CT_SUITES = emqx_sn_protocol

CT_NODE_NAME = emqxct@127.0.0.1
CT_OPTS = -cover test/ct.cover.spec -erl_args -name $(CT_NODE_NAME)

COVER = true

$(shell [ -f erlang.mk ] || curl -s -o erlang.mk https://raw.githubusercontent.com/emqx/erlmk/master/erlang.mk)
include erlang.mk

CUTTLEFISH_SCRIPT = _build/default/lib/cuttlefish/cuttlefish


app.config: $(CUTTLEFISH_SCRIPT) gen-config
	$(verbose) $(CUTTLEFISH_SCRIPT) -l info -e etc/ -c etc/emqx_sn.conf -i priv/emqx_sn.schema -d data

ct: app.config

rebar-cover:@rebar3 cover

coveralls:
	@rebar3 coveralls send

$(CUTTLEFISH_SCRIPT): rebar-deps
	@if [ ! -f cuttlefish ]; then make -C _build/default/lib/cuttlefish; fi

gen-config:
	@if [ -d deps/emqx ]; then make -C deps/emqx etc/gen.emqx.conf; \
		else make -C _build/default/lib/emqx etc/gen.emqx.conf; \
	fi

rebar-xref:
	@rebar3 xref

rebar-deps:
	@rebar3 get-deps

rebar-eunit: $(CUTTLEFISH_SCRIPT)
	@rebar3 eunit

rebar-compile:
	@rebar3 compile

rebar-ct: app.config
	@rebar3 as test compile
	@rebar3 ct -v --readable=false --name $(CT_NODE_NAME) --suite=$(shell echo $(foreach var,$(CT_SUITES),test/$(var)_SUITE) | tr ' ' ',')

rebar-clean:
	@rebar3 clean

distclean:: rebar-clean
	@rm -rf _build cover deps logs log data
	@rm -f rebar.lock compile_commands.json cuttlefish
