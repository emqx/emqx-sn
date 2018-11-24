PROJECT = emqx_sn
PROJECT_DESCRIPTION = EMQ X MQTT-SN Gateway
PROJECT_VERSION = 3.0

DEPS = esockd clique
dep_esockd = git-emqx https://github.com/emqx/esockd emqx30
dep_clique = git-emqx https://github.com/emqx/clique develop

BUILD_DEPS = emqx cuttlefish
dep_emqx = git-emqx https://github.com/emqx/emqx emqx30
dep_cuttlefish = git-emqx https://github.com/emqx/cuttlefish emqx30

NO_AUTOPATCH = cuttlefish

ERLC_OPTS += +debug_info

TEST_ERLC_OPTS += +debug_info
TEST_ERLC_OPTS += +'{parse_transform}'

CT_SUITES = emqx_sn_protocol

CT_NODE_NAME = emqxct@127.0.0.1
CT_OPTS = -cover test/ct.cover.spec -erl_args -name $(CT_NODE_NAME)

COVER = true

define dep_fetch_git-emqx
	git clone -q --depth 1 -b $(call dep_commit,$(1)) -- $(call dep_repo,$(1)) $(DEPS_DIR)/$(call dep_name,$(1)) > /dev/null 2>&1; \
	cd $(DEPS_DIR)/$(call dep_name,$(1));
endef

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

## Below are for version consistency check during erlang.mk and rebar3 dual mode support
none=
space = $(none) $(none)
comma = ,
quote = \"
curly_l = "{"
curly_r = "}"
dep-versions = [$(foreach dep,$(DEPS) $(BUILD_DEPS),$(curly_l)$(dep),$(quote)$(word 3,$(dep_$(dep)))$(quote)$(curly_r)$(comma))[]]

.PHONY: dep-vsn-check
dep-vsn-check:
	$(verbose) erl -noshell -eval \
		"MkVsns = lists:sort(lists:flatten($(dep-versions))), \
		{ok, Conf} = file:consult('rebar.config'), \
		{_, Deps1} = lists:keyfind(deps, 1, Conf), \
		{_, Deps2} = lists:keyfind(github_emqx_deps, 1, Conf), \
		F = fun({N, V}) when is_list(V) -> {N, V}; ({N, {git, _, {branch, V}}}) -> {N, V} end, \
		RebarVsns = lists:sort(lists:map(F, Deps1 ++ Deps2)), \
		case {RebarVsns -- MkVsns, MkVsns -- RebarVsns} of \
		  {[], []} -> halt(0); \
		  {Rebar, Mk} -> erlang:error({deps_version_discrepancy, [{rebar, Rebar}, {mk, Mk}]}) \
		end."
