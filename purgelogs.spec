%global         sum A script to purge logs.

Name:           purgelogs
Version:        0.0.1
Release:        1%{?dist}
Summary:        %{sum}

License:        ASL 2.0
URL:            https://docs.softwarefactory-project.io/%{name}
Source0:        https://tarballs.softwarefactory-project.io/%{name}/%{name}-%{version}.tar.gz

BuildArch:      noarch

Buildrequires:  python3-devel

Requires:       python3

%description
%{sum}

%prep
%autosetup -n %{name}-%{version}

%build
%{__python3} setup.py build

%install
%{__python3} setup.py install --skip-build --root %{buildroot}

%files
%{python3_sitelib}/*
%{_bindir}/*

%changelog
* Mon Sep 14 2020 Tristan Cacqueray <tdecacqu@redhat.com> - 0.0.1-1
- Initial packaging
