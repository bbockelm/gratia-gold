
Name: gratia-gold
Summary: A converter script from a Gratia database into Gold
Version: 0.1
License: ASL 2.0
Release: 2%{?dist}
Group: System Environment/Libraries

BuildArch: noarch
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot

Source0: %{name}-%{version}.tar.gz

%description
%{summary}

%prep
%setup -q

%build
python setup.py build

%install
rm -rf $RPM_BUILD_ROOT

python setup.py install --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

# Ghost files for the RPM.
mkdir -p $RPM_BUILD_ROOT/%_localstatedir/log/gratia-gold
touch $RPM_BUILD_ROOT/%_localstatedir/log/gratia-gold/gratia-gold.log

mkdir -p $RPM_BUILD_ROOT/%_localstatedir/lock/
touch $RPM_BUILD_ROOT/%_localstatedir/lock/gratia-gold.lock

%clean
rm -rf $RPM_BUILD_ROOT

%pre

getent group gold >/dev/null || groupadd -r gold
getent passwd gold >/dev/null || \
    useradd -r -g gold -d /var/lib/gratia-gold -s /sbin/nologin \
    -c "User for running gold" gold
exit 0

%files -f INSTALLED_FILES
%defattr(-,root,root)
%config(noreplace) %_sysconfdir/gratia-gold.cfg
%dir %_localstatedir/log/gratia-gold
%ghost %_localstatedir/log/gratia-gold/gratia-gold.log
%ghost %_localstatedir/lock/gratia-gold.lock

%changelog
* Tue Mar 06 2012 Brian Bockelman <bbockelm@cse.unl.edu> - 0.1-2
- Initial packaging of the gratia-gold package.

