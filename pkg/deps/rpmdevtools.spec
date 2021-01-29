Name: rpmdevtools
Version: 8.9
Release: 1%{?dist}
Summary: Tools to aid in package development

Group: Software
License: GPLv3
URL: https://fedoraproject.org/wiki/Rpmdevtools
Source0: https://fedorahosted.org/releases/r/p/rpmdevtools/rpmdevtools-%{version}.tar.xz

#BuildRequires:
#Requires:

%description
rpmdevtools contains many scripts to aid in package development.

%dump
exit 1

%prep
%setup -q

%build
%configure
make %{?_smp_mflags} V=0


%install
%make_install


%clean
rm -rf $RPM_BUILD_ROOT

%post
ldconfig

%preun


%files
%defattr(-,root,root,-)
/usr/*
/etc/rpmdevtools/*
/etc/bash_completion.d/rpmdevtools.bash-completion

%doc

%changelog
