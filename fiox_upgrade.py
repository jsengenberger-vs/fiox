import os
import command

print commands.getoutput("wget https://github.com/jsengenberger-vs/fiox/archive/master.zip")
print commands.getoutput("unzip master.zip")
print commands.getoutput("mv ./fiox-master/* .")
