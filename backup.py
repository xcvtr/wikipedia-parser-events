import os
import shutil
from datetime import datetime

def create_backup():
    """Создает резервную копию проекта"""
    # Создаем директорию для бэкапов, если её нет
    backup_dir = 'backups'
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir)
    
    # Генерируем имя файла с текущей датой и временем
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Список файлов для резервного копирования
    files_to_backup = [
        'disaster_parser.py',
        'state.txt',
        'disasters.csv'  # если файл существует
    ]
    
    # Создаем поддиректорию для текущего бэкапа
    backup_path = os.path.join(backup_dir, f'backup_{timestamp}')
    os.makedirs(backup_path)
    
    # Копируем файлы
    for file in files_to_backup:
        if os.path.exists(file):
            shutil.copy2(file, os.path.join(backup_path, file))
            print(f'Создана резервная копия файла: {file}')
    
    print(f'\nРезервная копия создана в директории: {backup_path}')

def restore_backup(backup_name):
    """Восстанавливает проект из резервной копии"""
    backup_path = os.path.join('backups', backup_name)
    
    if not os.path.exists(backup_path):
        print(f'Ошибка: Резервная копия {backup_name} не найдена')
        return
    
    # Список файлов для восстановления
    files_to_restore = [
        'disaster_parser.py',
        'state.txt',
        'disasters.csv'
    ]
    
    # Восстанавливаем файлы
    for file in files_to_restore:
        backup_file = os.path.join(backup_path, file)
        if os.path.exists(backup_file):
            shutil.copy2(backup_file, file)
            print(f'Восстановлен файл: {file}')
    
    print(f'\nПроект восстановлен из резервной копии: {backup_name}')

def list_backups():
    """Выводит список доступных резервных копий"""
    backup_dir = 'backups'
    if not os.path.exists(backup_dir):
        print('Резервные копии не найдены')
        return
    
    backups = [d for d in os.listdir(backup_dir) if os.path.isdir(os.path.join(backup_dir, d))]
    if not backups:
        print('Резервные копии не найдены')
        return
    
    print('Доступные резервные копии:')
    for backup in sorted(backups, reverse=True):
        print(f'- {backup}')

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 2:
        print('Использование:')
        print('  python backup.py create  - создать резервную копию')
        print('  python backup.py list    - показать список резервных копий')
        print('  python backup.py restore <backup_name>  - восстановить из резервной копии')
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'create':
        create_backup()
    elif command == 'list':
        list_backups()
    elif command == 'restore':
        if len(sys.argv) < 3:
            print('Ошибка: Укажите имя резервной копии')
            sys.exit(1)
        restore_backup(sys.argv[2])
    else:
        print(f'Неизвестная команда: {command}')
        sys.exit(1) 