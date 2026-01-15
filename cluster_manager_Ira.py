
"""
Скрипт для управления кластером серверов 1С:Предприятие с диагностикой
"""

import subprocess
import re
import json
import os
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from typing import List, Dict, Optional
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cluster_report.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


@dataclass
class ClusterConfig:
    """Конфигурация подключения к кластеру"""
    rac_path: str = r"C:\Program Files\1cv8\8.3.22.2239\bin\rac.exe"
    ras_host: str = "localhost"
    ras_port: int = 1545
    cluster_user: str = ""
    cluster_pwd: str = ""
    infobase_user: str = ""
    infobase_pwd: str = ""
    inactive_hours: int = 24


@dataclass
class Cluster:
    """Информация о кластере"""
    cluster_id: str
    host: str
    port: str
    name: str


@dataclass
class WorkingServer:
    """Информация о рабочем сервере"""
    server_id: str
    name: str
    host: str
    port: str
    port_range: str = ""
    cluster_port: str = ""


@dataclass
class InfoBase:
    """Информация об информационной базе"""
    infobase_id: str
    name: str
    descr: str = ""
    sessions_deny: str = "off"
    scheduled_jobs_deny: str = "off"
    sessions_count: int = 0
    last_session_time: Optional[datetime] = None
    is_inactive: bool = False


@dataclass
class Session:
    """Информация о сеансе"""
    session_id: str
    infobase_id: str
    user_name: str
    app_id: str
    started_at: Optional[datetime] = None
    last_active_at: Optional[datetime] = None


@dataclass
class ClusterReport:
    """Отчёт о состоянии кластера"""
    generated_at: datetime = field(default_factory=datetime.now)
    clusters: List[Cluster] = field(default_factory=list)
    servers: List[WorkingServer] = field(default_factory=list)
    infobases: List[InfoBase] = field(default_factory=list)
    sessions: List[Session] = field(default_factory=list)
    inactive_bases: List[InfoBase] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class RACClient:
    """Клиент для работы с утилитой RAC"""
    
    def __init__(self, config: ClusterConfig):
        self.config = config
        self._validate_rac_path()
    
    def _validate_rac_path(self):
        if not os.path.exists(self.config.rac_path):
            raise FileNotFoundError(f"Утилита RAC не найдена: {self.config.rac_path}")
    
    def _build_base_command(self) -> List[str]:
        return [
            self.config.rac_path,
            f"{self.config.ras_host}:{self.config.ras_port}"
        ]
    
    def _add_cluster_auth(self, cmd: List[str]) -> List[str]:
        if self.config.cluster_user:
            cmd.extend(["--cluster-user", self.config.cluster_user])
        if self.config.cluster_pwd:
            cmd.extend(["--cluster-pwd", self.config.cluster_pwd])
        return cmd
    
    def _add_infobase_auth(self, cmd: List[str]) -> List[str]:
        if self.config.infobase_user:
            cmd.extend(["--infobase-user", self.config.infobase_user])
        if self.config.infobase_pwd:
            cmd.extend(["--infobase-pwd", self.config.infobase_pwd])
        return cmd
    
    def _execute_command(self, cmd: List[str]) -> str:
        """Выполнить команду RAC с полной диагностикой"""
        try:
            logger.debug(f"Выполнение команды: {' '.join(cmd)}")
            print(f"\n🔍 КОМАНДА: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                timeout=30
            )
            
            print(f"📊 Return Code: {result.returncode}")
            
            if result.stdout:
                preview = result.stdout[:300] if len(result.stdout) > 300 else result.stdout
                print(f"📤 Stdout (первые 300 символов):\n{preview}")
            else:
                print(f"📤 Stdout: (пусто)")
            
            if result.stderr:
                preview = result.stderr[:300] if len(result.stderr) > 300 else result.stderr
                print(f"❌ Stderr:\n{preview}")
            
            if result.returncode != 0:
                logger.error(f"Error comand (код {result.returncode}): {result.stderr}")
                return ""
            
            if not result.stdout.strip():
                logger.warning(f"Comand wokring, but the output in empty!")
                return ""
            
            print(f"The data was recieved successfully\n")
            return result.stdout
            
        except subprocess.TimeoutExpired:
            print(f"Timeout: команда выполнялась более 30 секунд")
            logger.error("Timeout выполнения команды (30 сек)")
            return ""
        except Exception as e:
            print(f"Erorr krit: {e}")
            logger.error(f"Erorr: {e}")
            return ""
    
    def _parse_output(self, output: str) -> List[Dict[str, str]]:
        items = []
        current_item = {}
        
        for line in output.strip().split('\n'):
            line = line.strip()
            if not line:
                if current_item:
                    items.append(current_item)
                    current_item = {}
                continue
            
            if ':' in line:
                key, _, value = line.partition(':')
                current_item[key.strip()] = value.strip()
        
        if current_item:
            items.append(current_item)
        
        return items
    
    def get_clusters(self) -> List[Cluster]:
        cmd = self._build_base_command() + ["cluster", "list"]
        output = self._execute_command(cmd)
        
        clusters = []
        for item in self._parse_output(output):
            clusters.append(Cluster(
                cluster_id=item.get('cluster', ''),
                host=item.get('host', ''),
                port=item.get('port', ''),
                name=item.get('name', '')
            ))
        
        logger.info(f"Find clusters: {len(clusters)}")
        return clusters
    
    def get_servers(self, cluster_id: str) -> List[WorkingServer]:
        cmd = self._build_base_command() + [
            "server", "list",
            f"--cluster={cluster_id}"
        ]
        cmd = self._add_cluster_auth(cmd)
        output = self._execute_command(cmd)
        
        servers = []
        for item in self._parse_output(output):
            servers.append(WorkingServer(
                server_id=item.get('server', ''),
                name=item.get('name', ''),
                host=item.get('host', ''),
                port=item.get('port', ''),
                port_range=item.get('port-range', ''),
                cluster_port=item.get('cluster-port', '')
            ))
        
        logger.info(f"Find working servers: {len(servers)}")
        return servers
    
    def get_infobases(self, cluster_id: str) -> List[InfoBase]:
        cmd = self._build_base_command() + [
            "infobase", "summary", "list",
            f"--cluster={cluster_id}"
        ]
        cmd = self._add_cluster_auth(cmd)
        output = self._execute_command(cmd)
        
        infobases = []
        for item in self._parse_output(output):
            infobases.append(InfoBase(
                infobase_id=item.get('infobase', ''),
                name=item.get('name', ''),
                descr=item.get('descr', '')
            ))
        
        logger.info(f"Find info bases: {len(infobases)}")
        return infobases
    
    def get_sessions(self, cluster_id: str, infobase_id: str = None) -> List[Session]:
        cmd = self._build_base_command() + [
            "session", "list",
            f"--cluster={cluster_id}"
        ]
        if infobase_id:
            cmd.append(f"--infobase={infobase_id}")
        cmd = self._add_cluster_auth(cmd)
        output = self._execute_command(cmd)
        
        sessions = []
        for item in self._parse_output(output):
            started_at = None
            last_active_at = None
            
            if item.get('started-at'):
                try:
                    started_at = datetime.fromisoformat(item['started-at'].replace('T', ' '))
                except:
                    pass
            
            if item.get('last-active-at'):
                try:
                    last_active_at = datetime.fromisoformat(item['last-active-at'].replace('T', ' '))
                except:
                    pass
            
            sessions.append(Session(
                session_id=item.get('session', ''),
                infobase_id=item.get('infobase', ''),
                user_name=item.get('user-name', ''),
                app_id=item.get('app-id', ''),
                started_at=started_at,
                last_active_at=last_active_at
            ))
        
        logger.info(f"Sessions found: {len(sessions)}")
        return sessions
    
    def block_infobase(self, cluster_id: str, infobase_id: str, 
                       sessions_deny: bool = True, 
                       scheduled_jobs_deny: bool = True,
                       denied_message: str = "База заблокирована для обслуживания") -> bool:
        cmd = self._build_base_command() + [
            "infobase", "update",
            f"--cluster={cluster_id}",
            f"--infobase={infobase_id}",
            f"--sessions-deny={'on' if sessions_deny else 'off'}",
            f"--scheduled-jobs-deny={'on' if scheduled_jobs_deny else 'off'}",
            f"--denied-message={denied_message}"
        ]
        cmd = self._add_cluster_auth(cmd)
        cmd = self._add_infobase_auth(cmd)
        
        output = self._execute_command(cmd)
        success = "error" not in output.lower() if output else False
        
        if success:
            logger.info(f"База {infobase_id} заблокирована")
        else:
            logger.error(f"Не удалось заблокировать базу {infobase_id}")
        
        return success
    
    def unblock_infobase(self, cluster_id: str, infobase_id: str) -> bool:
        return self.block_infobase(
            cluster_id, infobase_id,
            sessions_deny=False,
            scheduled_jobs_deny=False,
            denied_message=""
        )
    
    def terminate_session(self, cluster_id: str, session_id: str) -> bool:
        cmd = self._build_base_command() + [
            "session", "terminate",
            f"--cluster={cluster_id}",
            f"--session={session_id}"
        ]
        cmd = self._add_cluster_auth(cmd)
        
        output = self._execute_command(cmd)
        return "error" not in output.lower() if output else True


class ClusterManager:
    """Менеджер управления кластером 1С"""
    
    def __init__(self, config: ClusterConfig):
        self.config = config
        self.client = RACClient(config)
        self.report = ClusterReport()
    
    def collect_cluster_info(self) -> ClusterReport:
        logger.info("=" * 50)
        logger.info("Начало сбора информации о кластере")
        logger.info("=" * 50)
        
        self.report = ClusterReport()
        
        try:
            self.report.clusters = self.client.get_clusters()
            
            for cluster in self.report.clusters:
                logger.info(f"Обработка кластера: {cluster.name} ({cluster.cluster_id})")
                
                servers = self.client.get_servers(cluster.cluster_id)
                self.report.servers.extend(servers)
                
                infobases = self.client.get_infobases(cluster.cluster_id)
                
                all_sessions = self.client.get_sessions(cluster.cluster_id)
                self.report.sessions.extend(all_sessions)
                
                now = datetime.now()
                inactive_threshold = now - timedelta(hours=self.config.inactive_hours)
                
                for ib in infobases:
                    ib_sessions = [s for s in all_sessions if s.infobase_id == ib.infobase_id]
                    ib.sessions_count = len(ib_sessions)
                    
                    if ib_sessions:
                        last_active_times = [
                            s.last_active_at for s in ib_sessions 
                            if s.last_active_at
                        ]
                        if last_active_times:
                            ib.last_session_time = max(last_active_times)
                    
                    if ib.sessions_count == 0:
                        ib.is_inactive = True
                        self.report.inactive_bases.append(ib)
                        logger.info(f"База '{ib.name}' неактивна (нет сеансов)")
                
                self.report.infobases.extend(infobases)
        
        except Exception as e:
            error_msg = f"Ошибка сбора информации: {e}"
            logger.error(error_msg)
            self.report.errors.append(error_msg)
        
        return self.report
    
    def block_inactive_bases(self, dry_run: bool = True) -> List[str]:
        blocked = []
        
        logger.info("=" * 50)
        logger.info(f"Блокировка неактивных баз ({'тестовый режим' if dry_run else 'БОЕВОЙ режим'})")
        logger.info("=" * 50)
        
        for cluster in self.report.clusters:
            for ib in self.report.inactive_bases:
                if dry_run:
                    logger.info(f"[DRY RUN] Будет заблокирована база: {ib.name}")
                    blocked.append(ib.name)
                else:
                    if self.client.block_infobase(
                        cluster.cluster_id, 
                        ib.infobase_id,
                        sessions_deny=True,
                        scheduled_jobs_deny=True,
                        denied_message="База автоматически заблокирована из-за неактивности"
                    ):
                        blocked.append(ib.name)
        
        return blocked
    
    def generate_report(self, output_format: str = 'text') -> str:
        if output_format == 'json':
            return self._generate_json_report()
        else:
            return self._generate_text_report()
    
    def _generate_text_report(self) -> str:
        lines = []
        lines.append("=" * 70)
        lines.append("ОТЧЁТ О СОСТОЯНИИ КЛАСТЕРА 1С:ПРЕДПРИЯТИЕ")
        lines.append(f"Дата формирования: {self.report.generated_at.strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 70)
        
        lines.append("\n### КЛАСТЕРЫ ###")
        for cluster in self.report.clusters:
            lines.append(f"  - {cluster.name}")
            lines.append(f"    ID: {cluster.cluster_id}")
            lines.append(f"    Хост: {cluster.host}:{cluster.port}")
        
        lines.append("\n### РАБОЧИЕ СЕРВЕРЫ ###")
        for server in self.report.servers:
            lines.append(f"  - {server.name}")
            lines.append(f"    ID: {server.server_id}")
            lines.append(f"    Хост: {server.host}:{server.port}")
        
        lines.append("\n### ИНФОРМАЦИОННЫЕ БАЗЫ ###")
        lines.append(f"Всего баз: {len(self.report.infobases)}")
        lines.append(f"Неактивных баз (без сеансов): {len(self.report.inactive_bases)}")
        lines.append("")
        
        for ib in self.report.infobases:
            status = "[НЕАКТИВНА]" if ib.is_inactive else "[АКТИВНА]"
            lines.append(f"  {status} {ib.name}")
            lines.append(f"    ID: {ib.infobase_id}")
            lines.append(f"    Сеансов: {ib.sessions_count}")
            if ib.last_session_time:
                lines.append(f"    Последняя активность: {ib.last_session_time}")
        
        lines.append("\n### АКТИВНЫЕ СЕАНСЫ ###")
        lines.append(f"Всего сеансов: {len(self.report.sessions)}")
        
        for session in self.report.sessions[:20]:
            lines.append(f"  - Пользователь: {session.user_name}")
            lines.append(f"    Приложение: {session.app_id}")
            if session.started_at:
                lines.append(f"    Начало: {session.started_at}")
        
        if len(self.report.sessions) > 20:
            lines.append(f"  ... и ещё {len(self.report.sessions) - 20} сеансов")
        
        if self.report.errors:
            lines.append("\n### ERRORS ###")
            for error in self.report.errors:
                lines.append(f"  ! {error}")
        
        lines.append("\n### RECOMMENDATIONS ###")
        if self.report.inactive_bases:
            lines.append("  - Рассмотрите возможность блокировки следующих неактивных баз:")
            for ib in self.report.inactive_bases:
                lines.append(f"    * {ib.name}")
        else:
            lines.append("  - Все базы активны, действий не требуется")
        
        lines.append("\n" + "=" * 70)
        lines.append("КОНЕЦ ОТЧЁТА")
        lines.append("=" * 70)
        
        return "\n".join(lines)
    
    def _generate_json_report(self) -> str:
        data = {
            'generated_at': self.report.generated_at.isoformat(),
            'clusters': [
                {
                    'id': c.cluster_id,
                    'name': c.name,
                    'host': c.host,
                    'port': c.port
                } for c in self.report.clusters
            ],
            'servers': [
                {
                    'id': s.server_id,
                    'name': s.name,
                    'host': s.host,
                    'port': s.port
                } for s in self.report.servers
            ],
            'infobases': [
                {
                    'id': ib.infobase_id,
                    'name': ib.name,
                    'sessions_count': ib.sessions_count,
                    'is_inactive': ib.is_inactive,
                    'last_session_time': ib.last_session_time.isoformat() if ib.last_session_time else None
                } for ib in self.report.infobases
            ],
            'sessions_count': len(self.report.sessions),
            'inactive_bases_count': len(self.report.inactive_bases),
            'errors': self.report.errors
        }
        return json.dumps(data, indent=2, ensure_ascii=False)
    
    def save_report(self, filename: str = None, output_format: str = 'text'):
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            ext = 'json' if output_format == 'json' else 'txt'
            filename = f"cluster_report_{timestamp}.{ext}"
        
        report_content = self.generate_report(output_format)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"Отчёт сохранён: {filename}")
        print(f"💾 Отчёт сохранён: {filename}\n")
        return filename


def main():
    """Главная функция"""
    
    print("\n" + "="*70)
    print("ПРОВЕРКА КОНФИГУРАЦИИ")
    print("="*70)
    
    config = ClusterConfig(
        rac_path=r"C:\Program Files (x86)\1cv8\8.3.27.1859\bin\rac.exe",
        ras_host="localhost",
        ras_port=1545,
        cluster_user="",
        cluster_pwd="",
        infobase_user="",
        infobase_pwd="",
        inactive_hours=24
    )
    
    print(f"✓ RAC путь: {config.rac_path}")
    print(f"  Файл существует: {os.path.exists(config.rac_path)}")
    print(f"✓ RAS хост: {config.ras_host}")
    print(f"✓ RAS порт: {config.ras_port}")
    print("="*70 + "\n")
    
    # ✅ ПРОВЕРКА соединения с RAS
    try:
        print("📡 Попытка подключения к RAS на localhost:1545...")
        client = RACClient(config)
        print("\n✅ Путь к RAC проверен успешно\n")
        
        clusters = client.get_clusters()
        print(f"✅ Подключение успешно!\n✓ Найдено кластеров: {len(clusters)}\n")
        
    except FileNotFoundError as e:
        print(f"❌ ОШИБКА: {e}")
        print("   Проверьте путь к rac.exe в конфигурации")
        return
    except Exception as e:
        print(f"❌ ОШИБКА подключения: {e}")
        print("   Убедитесь, что RAS запущена на localhost:1545")
        print("   Команда для запуска RAS:")
        print('   "C:\\Program Files (x86)\\1cv8\\8.3.27.1859\\bin\\ras.exe" cluster --port=1545 localhost:1540')
        return
    
    # ✅ ОСНОВНОЙ процесс
    try:
        print("="*70)
        print("СБОР ИНФОРМАЦИИ О КЛАСТЕРЕ")
        print("="*70 + "\n")
        
        manager = ClusterManager(config)
        report = manager.collect_cluster_info()
        
        print("\n" + "="*70)
        print("ГЕНЕРАЦИЯ ОТЧЁТОВ")
        print("="*70 + "\n")
        
        text_report = manager.generate_report('text')
        print(text_report)
        
        manager.save_report(output_format='text')
        manager.save_report(output_format='json')
        
        if report.inactive_bases:
            print("\n" + "=" * 70)
            print("⚠️  БЛОКИРОВКА НЕАКТИВНЫХ БАЗ (тестовый режим)")
            print("=" * 70)
            blocked = manager.block_inactive_bases(dry_run=True)
            print(f"⚠️  Будет заблокировано баз: {len(blocked)}\n")
        
        print("="*70)
        print("✅ СКРИПТ ЗАВЕРШЁН УСПЕШНО")
        print("="*70 + "\n")
        
    except Exception as e:
        logger.error(f"error krit: {e}")
        print(f"\nERROR KRIT: {e}\n")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
