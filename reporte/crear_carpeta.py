import os


def crear_carpeta(nombre):
    # Create directory
    dirName = nombre

    try:
        # Create target Directory
        os.mkdir(dirName)
        print("Directory ", dirName, " Created ")
    except:
        print("Directory ", dirName, " already exists")
