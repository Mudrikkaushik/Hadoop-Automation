import os
import getpass
import subprocess as sp
def hdfs_file(node,location):
    if location == "remote":
        if node == "namenode":

            os.system("echo '<?xml version=\"1.0\"?>' > /etc/hadoop/hdfs-site.xml")
            os.system("echo '<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")

            os.system("echo '<!-- Put site-specific property overrides in this file. -->' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<name>dfs.name.dir</name>' >> /etc/hadoop/hdfs-site.xml")
            name_dir = input("Enter the namenode folder name:")
            os.system("echo '<value>%s</value>' >> /etc/hadoop/hdfs-site.xml"%name_dir)
            os.system("echo '</property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '</configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("scp  /etc/hadoop/hdfs-site.xml 'root@%s':/etc/hadoop/hdfs-site.xml"%remoteip)

        elif node == "datanode":

            os.system("echo '<?xml version=\"1.0\"?>' > /etc/hadoop/hdfs-site.xml")
            os.system("echo '<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")

            os.system("echo '<!-- Put site-specific property overrides in this file. -->' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<name>dfs.data.dir</name>' >> /etc/hadoop/hdfs-site.xml")
            data_dir = input("Enter the datanode folder name:")
            os.system("echo '<value>%s</value>' >> /etc/hadoop/hdfs-site.xml"%data_dir)
            os.system("echo '</property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '</configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("scp  /etc/hadoop/hdfs-site.xml 'root@%s':/etc/hadoop/hdfs-site.xml"%remoteip)

    elif location == "local":
        if node == "namenode":

            os.system("echo '<?xml version=\"1.0\"?>' > /etc/hadoop/hdfs-site.xml")
            os.system("echo '<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")

            os.system("echo '<!-- Put site-specific property overrides in this file. -->' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<name>dfs.name.dir</name>' >> /etc/hadoop/hdfs-site.xml")
            name_dir = input("Enter the namenode folder name:")
            os.system("echo '<value>%s</value>' >> /etc/hadoop/hdfs-site.xml"%name_dir)
            os.system("echo '</property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '</configuration>' >> /etc/hadoop/hdfs-site.xml")

        elif node == "datanode":

            os.system("echo '<?xml version=\"1.0\"?>' > /etc/hadoop/hdfs-site.xml")
            os.system("echo '<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")

            os.system("echo '<!-- Put site-specific property overrides in this file. -->' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo ' ' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<configuration>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '<name>dfs.data.dir</name>' >> /etc/hadoop/hdfs-site.xml")
            data_dir = input("Enter the datanode folder name:")
            os.system("echo '<value>%s</value>' >> /etc/hadoop/hdfs-site.xml"%data_dir)
            os.system("echo '</property>' >> /etc/hadoop/hdfs-site.xml")
            os.system("echo '</configuration>' >> /etc/hadoop/hdfs-site.xml")

def menu():
    print("\t\tWelcome To hadoop Automation")

    print("""
Press 1: To create hdfs cluster(change in hdfs file)
Press 2: To format  namenode
Press 3: To create hdfs cluster(change in core file).
Press 4: To start (namenode/datanode).
Press 5: To stop (namenode/datanode).
Press 6: To store file in datanode.
Press 7: To retrieve file stored in datanode.
Press 8: To get report of cluster
press 9: To List all Attached Disk.
press 10: To List all Physical Volume.
press 11: To List all Volume Group.
press 12: To List all Logical Volume.
press 13: To Create Physical Volume.
press 14: To Create Volume Group.
press 15: TO Create Logical Volume.
press 16: To Extend volume.
Press 17: To Exit.
            
            """)
    os.system("tput setaf 7")

def conditions():
    if int(ch) == 1:
        print("you press {} here is your requested command".format(ch))
        print("\n")
        node = input("what do you want to configer<namenode/datanode>:")
        os.system("tput setaf 6")
        hdfs_file(node,location)

       
    elif int(ch) == 2:
        print("you press {} here is your requested command".format(ch))
        os.system("tput setaf 6")
        print("\n")
        os.system("hadoop namenode -format")

	    
    elif int(ch) == 3:
        print("you press {} here is your requested command".format(ch))
        print("\n")
        node = input("what do you want to configer<namenode/datanode>:")
        os.system("tput setaf 6")
        core_file(node,location)


    elif int(ch) == 4:
        print("you press {} here is your requested command".format(ch))
        print("\n")
        nodestart = input("what do you want to start(namenode/datanode):")
        os.system("tput setaf 6")
        os.system("hadoop-daemon.sh start {}".format(nodestart))
        os.system("jps")


    elif int(ch) == 5:

        print("you press {} here is your requested command".format(ch))
        print("\n")
        nodestop = input("what do you want to stop(namenode/datanode):")
        os.system("tput setaf 6")
        os.system("hadoop-daemon.sh stop {}".format(nodestop))
        os.system("jps")
    elif int(ch) == 6:

        print("you press {} here is your requested command".format(ch))
        print("\n")
        file_name = input("Your file name:")
        os.system("tput setaf 6")
        os.system("hadoop fs -put {} /".format(file_name))

    elif int(ch) == 7:
        print("you press {} here is your requested file".format(ch))
        os.system("tput setaf 6")
        print("\n")
        os.system("hadoop fs -ls  /")
        file_name = input("Your file name:")
        os.system("tput setaf 6")
        os.system("hadoop fs -cat /{}".format(file_name))
        os.system("jps")
    elif int(ch) == 8:
        print("you press {} here is your requested file".format(ch))
        os.system("tput setaf 6")
        print("\n")
        os.system("hadoop dfsadmin -report | less")

    elif int(ch)== 9:
        print("you press {} here is your requested file".format(ch))
        print("\n")
        os.system("tput setaf 6")
        os.system('fdisk -l | less')
    elif int(ch)== 10:
        print("you press {} here is your requested file".format(ch))
        print("\n")
        os.system("tput setaf 6")
        os.system('pvdisplay | less')
    elif int(ch)== 11:
        print("you press {} here is your requested file".format(ch))
        print("\n")
        os.system("tput setaf 6")
        os.system('vgdisplay | less')   
    elif int(ch)== 12:
        print("you press {} here is your requested file".format(ch))
        print("\n")
        os.system("tput setaf 6")
        os.system('lvdisplay | less')  

    elif int(ch) == 13:
        os.system('clear')
        print("\t\t\t\tPHYSICAL VOLUME CREATION ")
        os.system("tput setaf 6")
        print("\t\t\t\t-------------------------")
        print()
        name=input("\t\t\t\tEnter the hard disk name : ")
        os.system("tput setaf 6")
        x=sp.getstatusoutput('pvcreate {}'.format(name))
        print(x)
        if x[0]==0:
            print('\t\t\t\tPHYSICAL VOLUME CREATED')
            os.system("tput setaf 6")
            y=sp.getstatusoutput('pvdisplay {}'.format(name))
            print(y[1])
        else :
            print("PROCESS FAIL")
            os.system("tput setaf 2")
    elif int(ch) == 14:
        os.system('clear')
        print("\t\t\t\tVOLUME GROUP CREATION ")
        print("\t\t\t\t----------------------")
        name=input("\t\t\t\tEnter Volume Group name : ")
        name1=input("\t\t\t\tEnter the first Physical Volume name : ")
        name2=input("\t\t\t\tEnter the second Physical Volume name : ")
        os.system("tput setaf 6")
        x=sp.getstatusoutput('vgcreate {0} {1} {2}'.format(name,name1,name2))
        print(x)
        if x[0]==0:
            print('\t\t\t\tVOLUME GROUP CREATED')
            y=sp.getstatusoutput('vgdisplay {}'.format(name))
            print(y[1])
        else :
            print("PROCESS FAIL")

    elif int(ch) == 15:
        os.system('clear')
        print("\t\t\t\tLOGICAL VOLUME CREATION ")
        print("\t\t\t\t------------------------")
        vname=input("\t\t\t\tEnter Volume Group name : ")
        lsize=input("\t\t\t\tEnter Size: ")
        os.system("tput setaf 6")
        x=sp.getstatusoutput('lvcreate --size {0}G --name {1} '.format(lsize,vname))
        print(x)                                                                         
        if x[0]==0:
            print('\t\t\t\tLOGICAL VOLUME CREATED')
            y=sp.getstatusoutput('lvdisplay {}'.format(name))
            print(y[1])
        else :
            print("PROCESS FAIL")

    elif int(ch) == 16:
        print("How much size you want to increase?")
        isize=input("Enter size(in GB): ")
        z=input("Enter the Logical Volume Name: ")
        os.system("tput setaf 6")
        x=sp.getstatusoutput("lvextend --size +{0}G  {1}".format(isize,z))
        print(x)
        if x[0]==0:
            y=sp.getoutput("resize2fs  {0}".format(z))
            print("SIZE INCREASE SUCCESSFULLY BY {} GB".format(isize))
        else:
            print("INCREASE FAIL")


    elif int(ch) == 17:
        exit()
    else:	
        print("""
                \t.....Wrong input.....
                """)
        os.system("tput setaf 2")
    input("Press Enter to continue..........")
    os.system("clear")	

def menu2():
    os.system("clear")
    print("\t\tWelcome To Hadoop Automation")
    os.system("tput setaf 7")

menu2()
print("\n")
location = input("select your location where you want to run your code(local):")
os.system("tput setaf 3")
print("\n")
if location == "remote":
    remoteip = input("Enter your remote IP:")
while True:        
        
        os.system("clear")
        menu()
        
        if location == "local":
            print("\n")	
            ch = input("Enter your choice:")        
            conditions()       
        elif location == "remote":
            print("\n")
            ch = input("Enter your choice:")
 
while True:
    
        os.system("clear")
        menu()
        
        if location == "local":
            print("\n")	
            ch = input("Enter your choice:")        
            conditions()        
            

