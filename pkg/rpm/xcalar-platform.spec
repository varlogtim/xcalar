Name:           xcalar-platform
License:        Proprietary
Group:          Applications/Databases
Summary:        Xcalar Platform Dependencies. Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
Vendor:         Xcalar, Inc.
Version:        %{_version}
Release:        %{_build_number}%{?dist}
URL:            http://www.xcalar.com
Packager:       Xcalar, Inc. <support@xcalar.com>
Buildroot:      %{_tmppath}/%{name}-root
Prefix:         %prefix

%if 0%{?centos} >= 7 || 0%{?rhel} >= 7
Requires:       snappy nfs-utils rsyslog util-linux python2 initscripts net-tools bind-utils libxml2 libxslt openssl iproute glibc-common openldap bzip2-libs virt-what xz-libs cyrus-sasl cyrus-sasl-lib cyrus-sasl-gssapi cyrus-sasl-md5 krb5-libs krb5-workstation firewalld-filesystem libcgroup-tools
%endif
%if "0%{?amzn1}" == "01" || "0%{?amzn2}" == "01"
Requires:       snappy nfs-utils rsyslog dbus-glib hwdata initscripts iproute iptables iputils libdrm libpciaccess policycoreutils udev util-linux-ng net-tools which gawk bind-utils libxml2 libxslt openssl glibc-common openldap bzip2-libs virt-what xz-libs cyrus-sasl cyrus-sasl-lib cyrus-sasl-gssapi cyrus-sasl-md5
%endif

Requires:       freetds unixODBC mysql-connector-odbc mysql-libs postgresql-odbc postgresql-libs openblas cronie libcgroup sysstat

%if 0%{?centos} >= 7 || 0%{?rhel} >= 7 || "0%{?amzn2}" == "01"
Requires:       tuned
%endif

Requires(pre): shadow-utils

%description
Xcalar Data Platform Dependencies

%files
