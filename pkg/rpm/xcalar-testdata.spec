Name:           xcalar-testdata
License:        Proprietary
Group:          Applications/Databases
Summary:        Xcalar Platform Test Data. Copyright 2016 - 2019 Xcalar, Inc. All rights reserved.
Vendor:         Xcalar, Inc.
Version:        %{_version}
Release:        %{_build_number}
URL:            http://www.xcalar.com
Packager:       Xcalar, Inc. <support@xcalar.com>
Buildroot:      %{_tmppath}/%{name}-root
BuildArch:      noarch
Prefix:         %prefix

%description
Xcalar Data Platform Test Dataset

%build

%install
TEST_DATA_DIR="opt/xcalar/test_data"
mkdir -p %{buildroot}/${TEST_DATA_DIR}/
tar zxf %{_xlrdir}/src/data/qa/flight.tar.gz -C %{buildroot}/${TEST_DATA_DIR}/
tar zxf %{_xlrdir}/src/data/qa/yelp.tar.gz -C %{buildroot}/${TEST_DATA_DIR}/
tar zxf  %{_xlrdir}/src/data/qa/gdelt-small.tar.gz -C %{buildroot}/${TEST_DATA_DIR}/
mkdir %{buildroot}/${TEST_DATA_DIR}/indexJoin
cp -r %{_xlrdir}/src/data/qa/indexJoin/schedule %{buildroot}/${TEST_DATA_DIR}/indexJoin
mkdir %{buildroot}/${TEST_DATA_DIR}/jsonRandom
cp -r %{_xlrdir}/src/data/qa/jsonRandom/* %{buildroot}/${TEST_DATA_DIR}/jsonRandom
mkdir %{buildroot}/${TEST_DATA_DIR}/liblog
cp -r %{_xlrdir}/src/data/qa/liblog/* %{buildroot}/${TEST_DATA_DIR}/liblog

TAR_TESTDATA=%{_xlrdir}/build/xce/%{name}-%{_version}-%{_build_number}.tar.gz
mkdir -p %{_xlrdir}/build/xce
fakeroot tar czf "$TAR_TESTDATA" -C %{buildroot}/opt/xcalar/ test_data

%clean
rm -rf $RPM_BUILD_ROOT

%files
/opt/xcalar/test_data/*

%changelog
