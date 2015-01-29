import plumbum
from plumbum import SshMachine

def main():
    rem = SshMachine("monarch")
    dir = rem.path('/homes/gws/darioush/t/')
    with rem.cwd(dir):
        print rem['worker.sh']()



if __name__ == "__main__":
        main()

