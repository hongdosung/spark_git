import argparse
import os
import yaml
import traceback

parser = argparse.ArgumentParser()
parser.add_argument('--branch', '-b', required=True, help='branch name input')
args = parser.parse_args()
branch_nm = args.branch

class ApplicationConfig:
    def __init__(self, repository_nm='spark-hds'):
        try:
            self.repository_nm = repository_nm
            self.branch_root = f'/c/vscode/{repository_nm}'
            print(f'경로: {self.branch_root}')
            self.config = self._set_config(branch_nm)
        except:
            traceback_msg = traceback.format_exc()
            print(traceback_msg)

    def _set_config(self, branch_nm):
        if branch_nm.lower() == 'develop':
            branch_nm = 'dev'
        elif branch_nm.lower() == 'master':
            branch_nm = 'hds'
        
        with open(f'{self.branch_root}/deploy/application_{branch_nm}.yml') as f:
            return yaml.safe_load(f)

    
    def apply_config(self):
        for(root_dir, file_lst) in os.walk(self.branch_root):
            if len(file_lst) > 0:
                for file in file_lst:
                    if file.endswith('.pyc'):
                        continue
                    
                    with open(f'{root_dir}/{file}', mode='r+', encoding='utf-8') as f:
                        f_contents = ''.join(f.readlines())
                        replace_content = self.get_replace_contents(f_contents)
                        f.truncate(0)
                        f.seek(0) # truncate 후 첫 자리로 이동해야 garbage 데이터 남지 않음
                        f.write(replace_content)
                        
                    new_file_nm = self.get_replace_contents(file)
                    if file != new_file_nm:
                        os.rename(f'{root_dir}/{file}', f'{root_dir}/{new_file_nm}')

    
    def get_replace_contents(self, contents: str):
        key_lst = list(self.config.key())
        for main_subject in key_lst:
            for name, value in self.config.get(main_subject).items():
                contents = contents.replace(f'##{name}##', str(value))
        
        return contents
    
        
application_config = ApplicationConfig()
try:
    application_config.apply_config()
except:
    traceback_msg = traceback.format_exc()
    print(traceback_msg)
#eles:
#    print('deploy 완료')