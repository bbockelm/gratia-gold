

from distutils.core import setup

setup(name="gratia-gold",
      version="0.5",
      author="Brian Bockelman",
      author_email="bbockelman@cse.unl.edu",
      url="https://github.com/bbockelman/gratia-gold",
      description="Probe for synchronizing Gratia and GOLD",
      package_dir={"": "src"},
      packages=["gratia_gold"],

      scripts = ['src/gratia-gold'],

      data_files=[("/etc/cron.d", ["config/gratia-gold.cron"]),
            ("/etc/", ["config/gratia-gold.cfg"]),
            ("/etc/logrotate.d", ["config/gratia-gold.logrotate"]),
          ],

     )

