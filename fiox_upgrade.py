import os

#Won't work in python 3, but this works in 2
os.system("wget https://github.com/jsengenberger-vs/fiox/archive/master.zip")
os.system("unzip master.zip")
os.system("cp -R ./fiox-master/* .")
#os.system("mv ./fiox-master/tools/* ./tools/")
os.system("rm -rf ./fiox-master")
