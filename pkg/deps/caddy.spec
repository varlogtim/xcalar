# Original .spec from Federoa COPA, licensed under MIT.
#
# Modified by Xcalar, Inc to include GoogleDNS, AzureDNS and Route53
# providers.
%bcond_without realip
%bcond_without cloudflare
%bcond_without digitalocean
%bcond_without dyn
%bcond_without gandi
%bcond_without namecheap
%bcond_without powerdns
%bcond_without rackspace
%bcond_without rfc2136
%bcond_without googlecloud
%bcond_without route53
%bcond_without azure

%bcond_with debug

%if %{with debug}
%global _dwz_low_mem_die_limit 0
%else
%global debug_package %{nil}
%endif

# global systemd define
%if 0%{?fedora} >= 21 || 0%{?centos} >= 7 || 0%{?rhel} >= 7 || 0%{?suse_version} >= 1210
%global is_systemd 1
%endif


%if ! 0%{?gobuild:1}
%define gobuild(o:) /usr/local/bin/go build -ldflags "${LDFLAGS:-} -B 0x$(head -c20 /dev/urandom|od -An -tx1|tr -d ' \\n')" -v -x -tags netgo %{?**};
%endif

%if ! 0%{?goget:1}
%define goget(o:) /usr/local/bin/go get -ldflags "${LDFLAGS:-}" -v -x -tags netgo %{?**};
%endif

# caddy
%global import_path github.com/mholt/caddy

# realip
%global import_path_realip github.com/captncraig/caddy-realip
%global commit_realip 5dd1f4047d0f649f21ba9f8d7e491d712be9a5b0

# dnsproviders
%global import_path_dnsproviders github.com/caddyserver/dnsproviders
%global commit_dnsproviders 3fb56b86673f53871a9da68aeec2f2bdec06163c

# lego
%global import_path_lego github.com/xenolf/lego
%global commit_lego 4dde48a9b9916926a8dd4f69639c8dba40930355

# xcalar
%global xcalar_deps storage.googleapis.com/repo.xcalar.net/deps

Name: caddy
Version: 0.10.10
Release: %{iteration}%{?dist}
Summary: HTTP/2 web server with automatic HTTPS
License: ASL 2.0 and MIT
URL: https://caddyserver.com
Source0: https://%{import_path}/archive/v%{version}/caddy-%{version}.tar.gz
Source1: https://%{import_path_realip}/archive/%{commit_realip}/realip-%{commit_realip}.tar.gz
Source2: https://%{import_path_dnsproviders}/archive/%{commit_dnsproviders}/dnsproviders-%{commit_dnsproviders}.tar.gz
Source3: https://%{import_path_lego}/archive/%{commit_lego}/lego-%{commit_lego}.tar.gz
Source4: https://%{xcalar_deps}/caddy-support-%{version}-3.tar.gz
#Source10: caddy.conf
#Source11: caddy.service
#Source12: index.html
#ExclusiveArch: %%{go_arches}
# fails to build on s390x
ExclusiveArch: %{ix86} x86_64 %{arm} aarch64 ppc64le
#%{?go_compiler:BuildRequires: compiler(go-compiler)}
#BuildRequires: golang >= 1.8
%if 0%{?is_systemd}
BuildRequires: systemd
# %%{?systemd_requires}
%endif
Provides: webserver
# vendored libraries (Source0)
Provides: bundled(golang(cloud.google.com/go/compute/metadata)) = 7a4ba9f439fbc50061834a4063b57cf7222ba83f
Provides: bundled(golang(github.com/aead/chacha20)) = 8d6ce0550041f9d97e7f15ec27ed489f8bbbb0fb
Provides: bundled(golang(github.com/alecthomas/template)) = a0175ee3bccc567396460bf5acd36800cb10c49c
Provides: bundled(golang(github.com/alecthomas/units)) = 2efee857e7cfd4f3d0138cc3cbb1b4966962b93a
Provides: bundled(golang(github.com/codahale/aesnicheck)) = 349fcc471aaccc29cd074e1275f1a494323826cd
Provides: bundled(golang(github.com/dustin/go-humanize)) = 259d2a102b871d17f30e3cd9881a642961a1e486
Provides: bundled(golang(github.com/flynn/go-shlex)) = 3f9db97f856818214da2e1057f8ad84803971cff
Provides: bundled(golang(github.com/golang/protobuf/proto)) = 748d386b5c1ea99658fd69fe9f03991ce86a90c1
Provides: bundled(golang(github.com/golang/protobuf/ptypes/any)) = 748d386b5c1ea99658fd69fe9f03991ce86a90c1
Provides: bundled(golang(github.com/google/uuid)) = 7e072fc3a7be179aee6d3359e46015aa8c995314
Provides: bundled(golang(github.com/gorilla/websocket)) = a69d9f6de432e2c6b296a947d8a5ee88f68522cf
Provides: bundled(golang(github.com/hashicorp/go-syslog)) = 326bf4a7f709d263f964a6a96558676b103f3534
Provides: bundled(golang(github.com/hashicorp/golang-lru)) = 0a025b7e63adc15a622f29b0b2c4c3848243bbf6
Provides: bundled(golang(github.com/jimstudt/http-authentication/basic)) = 3eca13d6893afd7ecabe15f4445f5d2872a1b012
Provides: bundled(golang(github.com/lucas-clemente/aes12)) = 25700e67be5c860bcc999137275b9ef8b65932bd
Provides: bundled(golang(github.com/lucas-clemente/fnv128a)) = 393af48d391698c6ae4219566bfbdfef67269997
Provides: bundled(golang(github.com/lucas-clemente/quic-go)) = a9e2a28315406f825cdfe41f8652110addeb84a5
Provides: bundled(golang(github.com/lucas-clemente/quic-go-certificates)) = d2f86524cced5186554df90d92529757d22c1cb6
Provides: bundled(golang(github.com/miekg/dns)) = 0f3adef2e2201d72e50309a36fc99d8a9d1a4960
Provides: bundled(golang(github.com/naoina/go-stringutil)) = 6b638e95a32d0c1131db0e7fe83775cbea4a0d0b
Provides: bundled(golang(github.com/naoina/toml)) = e6f5723bf2a66af014955e0888881314cf294129
Provides: bundled(golang(github.com/russross/blackfriday)) = 067529f716f4c3f5e37c8c95ddd59df1007290ae
Provides: bundled(golang(github.com/xenolf/lego/acme)) = 4dde48a9b9916926a8dd4f69639c8dba40930355
Provides: bundled(golang(go4.org/syncutil/singleflight)) = 034d17a462f7b2dcd1a4a73553ec5357ff6e6c6e
Provides: bundled(golang(golang.org/x/crypto/acme)) = 2faea1465de239e4babd8f5905cc25b781712442
Provides: bundled(golang(golang.org/x/crypto/curve25519)) = 2faea1465de239e4babd8f5905cc25b781712442
Provides: bundled(golang(golang.org/x/crypto/hkdf)) = 2faea1465de239e4babd8f5905cc25b781712442
Provides: bundled(golang(golang.org/x/crypto/ocsp)) = 2faea1465de239e4babd8f5905cc25b781712442
Provides: bundled(golang(golang.org/x/crypto/ssh/terminal)) = 2faea1465de239e4babd8f5905cc25b781712442
Provides: bundled(golang(golang.org/x/net/context)) = f5079bd7f6f74e23c4d65efa0f4ce14cbd6a3c0f
Provides: bundled(golang(golang.org/x/net/http2)) = f5079bd7f6f74e23c4d65efa0f4ce14cbd6a3c0f
Provides: bundled(golang(golang.org/x/net/idna)) = f5079bd7f6f74e23c4d65efa0f4ce14cbd6a3c0f
Provides: bundled(golang(golang.org/x/net/lex/httplex)) = f5079bd7f6f74e23c4d65efa0f4ce14cbd6a3c0f
Provides: bundled(golang(golang.org/x/net/publicsuffix)) = f5079bd7f6f74e23c4d65efa0f4ce14cbd6a3c0f
Provides: bundled(golang(golang.org/x/oauth2)) = b53b38ad8a6435bd399ea76d0fa74f23149cca4e
Provides: bundled(golang(golang.org/x/sys/unix)) = 35ef4487ce0a1ea5d4b616ffe71e34febe723695
Provides: bundled(golang(golang.org/x/text/internal/gen)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/internal/triegen)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/internal/ucd)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/secure/bidirule)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/transform)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/unicode/bidi)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/unicode/cldr)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/unicode/norm)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(golang.org/x/text/unicode/rangetable)) = 836efe42bb4aa16aaa17b9c155d8813d336ed720
Provides: bundled(golang(google.golang.org/api/compute/v1)) = 66dba45b06824cbfe030e696b156d562994531e1
Provides: bundled(golang(google.golang.org/api/gensupport)) = 66dba45b06824cbfe030e696b156d562994531e1
Provides: bundled(golang(google.golang.org/api/googleapi)) = 66dba45b06824cbfe030e696b156d562994531e1
Provides: bundled(golang(google.golang.org/appengine)) = ad2570cd3913654e00c5f0183b39d2f998e54046
Provides: bundled(golang(gopkg.in/alecthomas/kingpin.v2)) = 1087e65c9441605df944fb12c33f0fe7072d18ca
Provides: bundled(golang(gopkg.in/natefinch/lumberjack.v2)) = df99d62fd42d8b3752c8a42c6723555372c02a03
Provides: bundled(golang(gopkg.in/square/go-jose.v1)) = aa2e30fdd1fe9dd3394119af66451ae790d50e0d
Provides: bundled(golang(gopkg.in/yaml.v2)) = 25c4ec802a7d637f88d584ab26798e94ad14c13b
# additional plugin libraries (Source1, Source2, Source3)
Provides: bundled(golang(github.com/captncraig/caddy-realip)) = %{commit_realip}
Provides: bundled(golang(github.com/caddyserver/dnsproviders)) = %{commit_dnsproviders}
Provides: bundled(golang(github.com/xenolf/lego)) = %{commit_lego}


%description
Caddy is the HTTP/2 web server with automatic HTTPS.  Official Caddy builds
with customized plugins can be downloaded from https://caddyserver.com.  This
package is an unofficial build with the following plugins:

%{?with_realip:  http.realip}
%{?with_cloudflare:  tls.dns.cloudflare}
%{?with_digitalocean:  tls.dns.digitalocean}
%{?with_dyn:  tls.dns.dyn}
%{?with_gandi:  tls.dns.gandi}
%{?with_namecheap:  tls.dns.namecheap}
%{?with_powerdns:  tls.dns.powerdns}
%{?with_rackspace:  tls.dns.rackspace}
%{?with_rfc2136:  tls.dns.rfc2136}
%{?with_azure:  tls.dns.azure}
%{?with_route53:  tls.dns.route53}
%{?with_googlecloud:  tls.dns.googlecloud}


%prep
%autosetup

%if %{with realip}
mkdir -p vendor/%{import_path_realip}
tar -C vendor/%{import_path_realip} --strip-components 1 -x -f %{S:1}
cp vendor/%{import_path_realip}/LICENSE LICENSE-realip
%endif

%if %{with cloudflare}%{with digitalocean}%{with dyn}%{with gandi}%{with namecheap}%{with powerdns}%{with rackspace}%{with rfc2136}
mkdir -p vendor/%{import_path_dnsproviders}
tar -C vendor/%{import_path_dnsproviders} --strip-components 1 -x -f %{S:2}
cp vendor/%{import_path_dnsproviders}/LICENSE LICENSE-dnsproviders
mkdir -p vendor/%{import_path_lego}
tar -C vendor/%{import_path_lego} --strip-components 1 -x -f %{S:3}
cp vendor/%{import_path_lego}/LICENSE LICENSE-lego
%endif

tar -x -f %{S:4}

sed -e '/other plugins/ a \\t// plugins added during rpmbuild' \
%{?with_realip:         -e '/other plugins/ a \\t_ "%{import_path_realip}"'} \
%{?with_cloudflare:     -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/cloudflare"'} \
%{?with_digitalocean:   -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/digitalocean"'} \
%{?with_dyn:            -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/dyn"'} \
%{?with_gandi:          -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/gandi"'} \
%{?with_namecheap:      -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/namecheap"'} \
%{?with_powerdns:       -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/pdns"'} \
%{?with_rackspace:      -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/rackspace"'} \
%{?with_rfc2136:        -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/rfc2136"'} \
%{?with_azure:          -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/azure"'} \
%{?with_route53:        -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/route53"'} \
%{?with_googlecloud:    -e '/other plugins/ a \\t_ "%{import_path_dnsproviders}/googlecloud"'} \
                        -i caddy/caddymain/run.go


%build
mkdir -p src/%(dirname %{import_path})
ln -s ../../.. src/%{import_path}
export GOPATH=$(pwd):/usr/share/gocode

export LDFLAGS="-X %{import_path}/caddy/caddymain.gitTag=v%{version}"
%goget github.com/Azure/go-autorest/autorest
%goget github.com/Azure/azure-sdk-for-go/arm/dns/...
%goget google.golang.org/api/dns/v1/...
%goget github.com/aws/aws-sdk-go/aws/client/...
%gobuild -o bin/caddy %{import_path}/caddy

%install
install -D -m 0755 bin/caddy %{buildroot}%{_bindir}/caddy
install -D -m 0644 caddy.conf %{buildroot}%{_sysconfdir}/caddy/caddy.conf
%if 0%{?is_systemd}
install -D -m 0644 caddy.service %{buildroot}%{_unitdir}/caddy.service
%else
install -D -m 0755 caddy.init %{buildroot}/etc/init.d/caddy
%endif
install -D -m 0644 index.html %{buildroot}%{_datadir}/caddy/index.html
install -d -m 0755 %{buildroot}%{_sysconfdir}/caddy/conf.d
install -d -m 0750 %{buildroot}%{_sharedstatedir}/caddy

%clean
rm -rf $RPM_BUILD_ROOT


%pre
getent group caddy &> /dev/null || \
groupadd -r caddy &> /dev/null
getent passwd caddy &> /dev/null || \
useradd -r -g caddy -d %{_sharedstatedir}/caddy -s /sbin/nologin -c 'Caddy web server' caddy &> /dev/null
exit 0


%post
setcap cap_net_bind_service=+ep %{_bindir}/caddy
%if 0%{?is_systemd}
%systemd_post caddy.service
%endif
exit


%preun
%if 0%{?is_systemd}
%systemd_preun caddy.service
%endif
exit

%postun
%if 0%{?is_systemd}
%systemd_postun_with_restart caddy.service
%else
/etc/init.d/caddy restart
%endif
exit


%files
%defattr(0644, root, root, 0755)
%doc dist/README.txt
%attr(0755, root,root) %{_bindir}/caddy
%{_datadir}/caddy
%if 0%{?is_systemd}
%{_unitdir}/caddy.service
%else
%dir /etc/init.d
%attr(0755, root, root) /etc/init.d/caddy
%endif
%dir %{_sysconfdir}/caddy
%dir %{_sysconfdir}/caddy/conf.d
%config(noreplace) %{_sysconfdir}/caddy/caddy.conf
%attr(0750,caddy,caddy) %dir %{_sharedstatedir}/caddy

%changelog
* Tue Jan 30 2018 Amit Bakshi <abakshi@xcalar.com> - 0.10.10-52
- Add EL6 support, googledns, azuredns, route53
- Add setcap cap_net_bind_service to caddy

* Thu Jan 11 2018 Carl George <carl@george.computer> - 0.10.10-2
- Add powerdns provider

* Mon Oct 09 2017 Carl George <carl@george.computer> - 0.10.10-1
- Latest upstream

* Mon Oct 02 2017 Carl George <carl@george.computer> - 0.10.9-6
- Add provides for bundled libraries

* Mon Oct 02 2017 Carl George <carl@george.computer> - 0.10.9-5
- Enable rfc2136 dns provider
- List plugins in description

* Mon Sep 18 2017 Carl George <carl@george.computer> - 0.10.9-4
- Exclude s390x

* Sun Sep 17 2017 Carl George <carl@george.computer> - 0.10.9-3
- Add realip plugin
- Add conditionals for plugins

* Sat Sep 16 2017 Carl George <carl@george.computer> - 0.10.9-2
- Add sources for caddyserver/dnsproviders and xenolf/lego
- Disable all dns providers that require additional libraries (dnsimple, dnspod, googlecloud, linode, ovh, route53, vultr)
- Rewrite default index.html

* Tue Sep 12 2017 Carl George <carl@george.computer> - 0.10.9-1
- Latest upstream
- Add config validation to unit file
- Disable exoscale dns provider https://github.com/xenolf/lego/issues/429

* Fri Sep 08 2017 Carl George <carl@george.computer> - 0.10.8-1
- Latest upstream
- Build with %%gobuild macro
- Move config subdirectory from /etc/caddy/caddy.conf.d to /etc/caddy/conf.d

* Tue Aug 29 2017 Carl George <carl@george.computer> - 0.10.7-1
- Latest upstream

* Fri Aug 25 2017 Carl George <carl@george.computer> - 0.10.6-2
- Use SIQQUIT to stop service
- Increase the process limit from 64 to 512
- Only `go get` in caddy/caddymain

* Fri Aug 11 2017 Carl George <carl@george.computer> - 0.10.6-1
- Latest upstream
- Add webserver virtual provides
- Drop tmpfiles and just own /var/lib/caddy directly
- Remove PrivateDevices setting from unit file, it prevents selinux process transitions
- Disable rfc2136 dns provider https://github.com/caddyserver/dnsproviders/issues/11

* Sat Jun 03 2017 Carl George <carl.george@rackspace.com> - 0.10.3-2
- Rename Envfile to envfile
- Rename Caddyfile to caddy.conf
- Include additional configs from caddy.conf.d directory

* Fri May 19 2017 Carl George <carl.george@rackspace.com> - 0.10.3-1
- Latest upstream

* Mon May 15 2017 Carl George <carl.george@rackspace.com> - 0.10.2-1
- Initial package
