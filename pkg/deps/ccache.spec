Name: ccache
Version: 3.3.2
Release: 4%{?dist}
Summary: A fast C/C++ compiler cache

Group: Software
License: GPLv3
URL: https://ccache.samba.org/
Source0: https://www.samba.org/ftp/ccache/ccache-%{version}.tar.gz

#BuildRequires:
#Requires:

%description
A fast C/C++ compiler cache

%dump
exit 1

%prep
%setup -q

%build
%configure
make %{?_smp_mflags} V=0


%install
%make_install
mkdir -p %{buildroot}/%{_prefix}/etc
touch %{buildroot}/%{_prefix}/etc/ccache.conf


%clean
rm -rf $RPM_BUILD_ROOT

%post
ldconfig

%preun


%files
%defattr(-,root,root,-)
/usr/bin/ccache
/usr/share/man/man1/ccache.1.gz
%config(noreplace) %attr(0755,root,root) /usr/etc/ccache.conf

%doc

%changelog
