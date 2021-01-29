.PHONY: all clean

SHELL=/bin/bash

VERSION := $(shell cat $(XLRDIR)/VERSION)
PYPKG := $(shell test -e /etc/system-release && rpm -qf /opt/xcalar/bin/python3 --qf '%{NAME}\n' 2>/dev/null || (dpkg -S /opt/xcalar/bin/python3 2>/dev/null | cut -d: -f1) )
PYPKGVER := $(shell test -e /etc/system-release && rpm -q $(PYPKG) --qf '%{VERSION}-%{RELEASE}\n' 2>/dev/null || (dpkg -l $(PYPKG) 2>/dev/null | tail -1 | awk '{print $$3}') )
PYVER := $(shell echo $(PYPKGVER) | cut -c1-3)

BUNDLE_VERSION := $(VERSION)-$(PYPKGVER)
ALL = pip-bundler-$(BUNDLE_VERSION).tar.gz pip-bundler-gs-$(BUNDLE_VERSION).zip

export PIP_FIND_LINKS=file:///netstore/infra/wheels/py$(PYVER)/index.html file:///netstore/infra/wheels/py$(PYVER)-el7/index.html

all: $(ALL)

requirements-$(BUNDLE_VERSION).txt: requirements.in
requirements-gs-$(BUNDLE_VERSION).txt: requirements-gs.in
%.txt:
	pip-compile $< -o $@.tmp -v --no-annotate
	(grep -Ev '^(#|$$)' $@.tmp | sed -r 's@(><=)@\\1@g' | xargs -I{} -n1 bash -c "grep -q '^{}' $(XLRDIR)/pkg/requirements.txt && echo '#{} ## Already present in $(PYPKG)-$(PYPKGVER)' || echo '{}'") | tee min-$@.tmp
	mv min-$@.tmp min-$@
	mv $@.tmp $@

pip-bundler-$(BUNDLE_VERSION).tar.gz: requirements-$(BUNDLE_VERSION).txt
	pip-bundler.sh -p /opt/xcalar/bin/python3 -r min-$< -c constraints.txt -o $@

pip-bundler-gs-$(BUNDLE_VERSION).zip: requirements-gs-$(BUNDLE_VERSION).txt
	pip-bundler.sh -p /opt/xcalar/bin/python3 -r min-$< -c constraints.txt -o $@

upload: $(ALL)
	for ii in $^; do gsutil -m cp $${ii} gs://repo.xcalar.net/deps/pip-bundler/; done

clean:
	rm -fv $(ALL) *-$(PYVER).*
