import os
import shutil

source_dir = os.getcwd()
target_dir = "../projects/{{ cookiecutter.project }}/"

shutil.move(source_dir, target_dir)
