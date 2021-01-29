.PHONY: clean

XLRGUI_GIT_SHA1:=$(shell cd $(XLRGUIDIR) && git rev-parse --short=8 HEAD)
GUI_BUILD_TARGET:=installer

ifeq ($(RC),rc)
	MAKE_EXTRA_PARAMS+=RC=true
endif

$(XLRGUIDIR)/xcalar-gui-$(XLRGUI_GIT_SHA1).tar.gz:
	cd $(XLRGUIDIR) && git submodule update && export PATH=node_modules/grunt-cli/bin:$(PATH) && make $(GUI_BUILD_TARGET) $(MAKE_EXTRA_PARAMS) PRODUCT=XD && fakeroot tar czf $@ xcalar-gui

node-modules: $(XLRGUIDIR)/xcalar-gui-$(XLRGUI_GIT_SHA1).tar.gz
	npm config set registry "http://registry.npmjs.org/"
	cd $(XLRGUIDIR)/xcalar-gui/services/expServer && npm install

node-modules.tar.gz: node-modules
	fakeroot tar czf $@ -C $(XLRGUIDIR)/xcalar-gui/services/expServer node_modules

xcalar-gui.tar.gz: $(XLRGUIDIR)/xcalar-gui-$(XLRGUI_GIT_SHA1).tar.gz
	rm -f xcalar-gui-*.tar.gz
	cp $< $@

gui-build: xcalar-gui.tar.gz node-modules.tar.gz
	rm -f $(XLRGUIDIR)/xcalar-gui-*.tar.gz

clean:
	rm -f xcalar-gui-*.tar.gz node-modules.tar.gz

