# import standard modules

# import third party modules

# import project related modules


from copy import deepcopy
from azure.storage.filedatalake import FileSystemClient, DataLakeServiceClient # for later DataLakeServiceClient


class AzureStorageExplorer:

    def __init__(self, file_sys, file_sys_name):
        self.file_system = FileSystemClient.from_connection_string(file_sys, file_system_name=file_sys_name)

    def _request_space(self):
        try:
            paths = self.file_system.get_paths()
            paths = [path.name for path in paths]
            return paths

        except Exception as E:
            print(E)
            print("Connection to Azure was not successfull, check file system and file system name")
            return list()

    def _build_nested_helper(self, path, text, container):
        segs = path.split('/')
        head = segs[0]
        tail = segs[1:]
        if not tail:
            container[head] = head
        else:
            if head not in container:
                container[head] = {}
            self._build_nested_helper('/'.join(tail), text, container[head])

    def _build_nested(self, paths):
        container = {}
        for path in paths:
            self._build_nested_helper(path, path, container)
        return container

    def _decode_files(self, obj):
        files = list()
        dirs = list()
        result = dict()
        for k, v in obj.items():
            if k == v:
                files.append(k)
            else:
                dirs.append({k: self._decode_files(v)})

        result["dirs"] = dirs
        result["files"] = files

        return result

    def get_json(self):

        # request files available in user specific Storage Account
        paths = self._request_space()

        container = self._build_nested(paths)
        container = self._decode_files(container)
        return container
