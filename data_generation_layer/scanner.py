from typing import List, Dict
import os
from data_generation_layer.models import File, FSOFactory

class DirectoryGroup:
    def __init__(self, name: str):
        self.name = name
        self.files: List[File] = []
        self.subdirectories: Dict[str, 'DirectoryGroup'] = {}

    def add_file(self, file: File):
        self.files.append(file)

    def add_subdirectory(self, subdir: 'DirectoryGroup'):
        self.subdirectories[subdir.name] = subdir

class DirectoryScanner:
    def __init__(self, dir_path: str) -> None:
        self.__dir_path = dir_path
        self.root = DirectoryGroup(os.path.basename(dir_path))
        self.fso_factory = FSOFactory()

    @property
    def dir_path(self) -> str:
        return self.__dir_path

    def scan(self) -> DirectoryGroup:
        self._scan_directory(self.__dir_path, self.root)
        return self.root

    def _scan_directory(self, path: str, current_group: DirectoryGroup):
        for item in os.scandir(path):
            if item.is_file():
                file = self.fso_factory.create_file(item.path)
                current_group.add_file(file)
            elif item.is_dir():
                subdir = self.fso_factory.create_dir(item.path)
                subdir_group = DirectoryGroup(subdir.name)
                current_group.add_subdirectory(subdir_group)
                self._scan_directory(item.path, subdir_group)

    def get_files_by_group(self) -> Dict[str, List[File]]:
        result = {}
        self._collect_files_by_group(self.root, result)
        return result

    def _collect_files_by_group(self, group: DirectoryGroup, result: Dict[str, List[File]], path: str = ""):
        current_path = os.path.join(path, group.name)
        result[current_path] = group.files
        for subdir in group.subdirectories.values():
            self._collect_files_by_group(subdir, result, current_path)

    def print_directory_structure(self):
        self._print_group(self.root)

    def _print_group(self, group: DirectoryGroup, indent: str = ""):
        print(f"{indent}{group.name}/")
        for file in group.files:
            print(f"{indent}  {file.name}")
        for subdir in group.subdirectories.values():
            self._print_group(subdir, indent + "  ")