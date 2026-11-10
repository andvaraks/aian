import asyncio
import importlib
import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
import httpx

scratch = tempfile.TemporaryDirectory()
os.environ.update(DATA_DIR=str(Path(scratch.name)/'data'), TEMP_DIR=str(Path(scratch.name)/'temp'),
                  COOKIE_SECURE='false', PUBLIC_ORIGIN='http://testserver')
import main as service


class AppTests(unittest.TestCase):
    def setUp(self):
        service.busy = False
        self.client = TestClient(service.app)
        self.client.__enter__()
        with service.db() as c:
            c.execute('DELETE FROM meeting')
            c.execute('DELETE FROM browser')
            c.execute('UPDATE app_state SET used=0')
        self.client.get('/api/session')

    def tearDown(self):
        self.client.__exit__(None, None, None)

    def post(self, filename='x.mp3', data=b'audio'):
        return self.client.post('/api/meetings', files={'file': (filename, data)}, headers={'Origin':'http://testserver'})

    def seed(self, owner=None):
        owner = owner or 'owner'
        mid = str(uuid.uuid4())
        with service.db() as c:
            c.execute('INSERT OR IGNORE INTO browser VALUES(?,?)', (owner, service.now()))
            c.execute("INSERT INTO meeting(id,owner,filename,created_at,status) VALUES(?,?,?,?, 'processing')", (mid,owner,'test.mp3',service.now()))
        return mid

    def test_origin_rejected(self):
        self.assertEqual(self.client.post('/api/meetings').status_code, 403)

    def test_invalid_extension_and_empty_do_not_consume(self):
        self.assertEqual(self.post('x.exe').status_code,415)
        self.assertEqual(self.post(data=b'').status_code,422)
        with service.db() as c:
            self.assertEqual(c.execute('SELECT used FROM app_state').fetchone()[0],0)

    def test_stream_size_rejected(self):
        response = self.client.post('/api/meetings', content=b'x', headers={'Origin':'http://testserver','Content-Length':str(service.MAX_BYTES+1048577)})
        self.assertEqual(response.status_code,413)

    def test_busy_no_consumption(self):
        service.busy=True
        self.assertEqual(self.post().status_code,409)

    def test_cloud_existing_result_before_quota(self):
        first = 'https://www.dropbox.com/scl/fi/file/meeting.mp4?dl=0&rlkey=key'
        equivalent = 'https://dl.dropboxusercontent.com/scl/fi/file/meeting.mp4?rlkey=key&raw=1'
        owner = service.hashlib.sha256(self.client.cookies.get(service.COOKIE).encode()).hexdigest()
        mid = self.seed(owner)
        service.consume(mid)
        key = service.hashlib.sha256(service.normalize_cloud_url(first).encode()).hexdigest()
        service.update(mid, status='ready', source='cloud', cloud_key=key)
        response = self.client.post('/api/meetings/cloud', json={'url': equivalent}, headers={'Origin': 'http://testserver'})
        self.assertEqual(response.json(), {'id': mid, 'status': 'ready', 'existing': True})
        self.assertEqual(len(self.client.get('/api/meetings').json()), 1)
        with service.db() as conn:
            self.assertEqual(conn.execute('SELECT used FROM app_state').fetchone()[0], 1)

    def test_cloud_redirect_rejects_external_host(self):
        requested = []

        def respond(request):
            requested.append(request.url.host)
            return httpx.Response(302, headers={'Location': 'http://127.0.0.1:8006/api/health'})

        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch.object(service.httpx, 'AsyncClient', return_value=client):
            with self.assertRaises(service.Problem) as error:
                asyncio.run(service.download_cloud('https://www.dropbox.com/s/file/meeting.mp4?dl=1', service.TEMP))
        self.assertEqual(error.exception.code, 'CLOUD_URL')
        self.assertEqual(requested, ['www.dropbox.com'])

    def test_cloud_download_error_preserves_quota(self):
        async def unavailable(url, directory):
            raise httpx.ConnectError('unavailable')

        with patch.object(service, 'download_cloud', unavailable):
            response = self.client.post('/api/meetings/cloud', json={'url': 'https://drive.google.com/file/d/test/view'},
                                        headers={'Origin': 'http://testserver'})
            self.assertEqual(response.status_code, 202)
            self.assertEqual(response.json()['stage'], 'fetching')
            mid = response.json()['id']
            for _ in range(50):
                detail = self.client.get('/api/meetings/' + mid).json()
                if detail['status'] == 'error':
                    break
                time.sleep(.01)
        self.assertEqual(detail['error_code'], 'CLOUD_DOWNLOAD')
        self.assertEqual(detail['source'], 'cloud')
        self.assertIsNone(detail['attempt_consumed_at'])
        self.assertTrue(self.client.get('/api/session').json()['available'])

    def test_cookie_and_cross_browser_access(self):
        cookie=self.client.cookies.get(service.COOKIE)
        owner=service.hashlib.sha256(cookie.encode()).hexdigest()
        mid=self.seed(owner)
        self.assertEqual(len(self.client.get('/api/meetings').json()),1)
        self.assertEqual(self.client.get('/api/meetings/'+mid).status_code,200)
        with TestClient(service.app) as other:
            self.assertEqual(other.get('/api/meetings').json(),[])
            self.assertEqual(other.get('/api/meetings/'+mid).status_code,404)

    def test_quota_idempotent_daily_and_global(self):
        mid=self.seed()
        service.consume(mid);service.consume(mid)
        with service.db() as c:
            self.assertEqual(c.execute('SELECT used FROM app_state').fetchone()[0],1)
        with self.assertRaises(service.Problem) as error:
            service.consume(self.seed())
        self.assertEqual(error.exception.code,'DAILY_LIMIT')
        with service.db() as c:c.execute('UPDATE app_state SET used=20')
        with self.assertRaises(service.Problem) as error:
            service.consume(self.seed('other'))
        self.assertEqual(error.exception.code,'APP_LIMIT_REACHED')

    def test_next_day(self):
        service.consume(self.seed())
        with patch.object(service,'day',return_value='2099-01-02'):
            service.consume(self.seed())

    def test_last_slot_atomic(self):
        ids=[self.seed('a'),self.seed('b')]
        with service.db() as c:c.execute('UPDATE app_state SET used=19')
        def consume(mid):
            try:service.consume(mid);return 'ok'
            except service.Problem:return 'blocked'
        with ThreadPoolExecutor(2) as executor:
            self.assertEqual(sorted(executor.map(consume,ids)),['blocked','ok'])
        with service.db() as c:self.assertEqual(c.execute('SELECT used FROM app_state').fetchone()[0],20)

    def test_restart_preserves_quota_and_marks_interrupted(self):
        mid=self.seed();service.consume(mid);service.initialize()
        with service.db() as c:
            self.assertEqual(c.execute('SELECT used FROM app_state').fetchone()[0],1)
            self.assertEqual(c.execute('SELECT status FROM meeting WHERE id=?',(mid,)).fetchone()[0],'error')

    def test_missing_initialized_database_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(service,'DATA',Path(directory)):
            (Path(directory)/'initialized').write_text('schema=1')
            service.storage_ok=False;service.initialize()
            self.assertFalse(service.storage_ok)
        service.storage_ok=True

    def test_summary_validation(self):
        valid={'summary':'Обсудили задачу','decisions':[],'tasks':[{'text':'Проверить','assignee':None,'deadline':None}],'open_questions':[]}
        self.assertEqual(service.parse_summary(json.dumps(valid)),valid)
        with self.assertRaises(service.Problem):service.parse_summary('{"summary": "bad"}')

    def test_pipeline_errors_preserve_spending_and_clean_temp(self):
        for spent in (False,True):
            with service.db() as c:c.execute('UPDATE app_state SET used=0');c.execute('DELETE FROM meeting')
            mid=self.seed();directory=Path(tempfile.mkdtemp(prefix='meeting-',dir=service.TEMP));path=directory/'source.mp3';path.write_bytes(b'audio')
            async def fail(path,mid):
                if spent:service.consume(mid)
                raise service.Problem('TEST','Ошибка проверки')
            with patch.object(service,'process_recording',fail):asyncio.run(service.run_job(path,mid))
            with service.db() as c:
                self.assertEqual(c.execute('SELECT used FROM app_state').fetchone()[0],int(spent))
                self.assertEqual(c.execute('SELECT status FROM meeting').fetchone()[0],'error')
            self.assertFalse(directory.exists())

    def test_upload_background_success_and_reload(self):
        async def fake_pipeline(path,mid):
            service.consume(mid)
            service.update(mid,status='ready',transcript='Обсудили запуск.',summary_json=json.dumps({'summary':'Обсудили запуск.','decisions':[],'tasks':[],'open_questions':[]}))
        with patch.object(service,'process_recording',fake_pipeline):
            response=self.post()
            self.assertEqual(response.status_code,202)
            mid=response.json()['id']
            for _ in range(30):
                result=self.client.get('/api/meetings/'+mid).json()
                if result['status']=='ready':break
                time.sleep(.01)
        self.assertEqual(result['status'],'ready')
        self.assertEqual(result['transcript'],'Обсудили запуск.')
        self.assertEqual(self.post().status_code,429)
        service.initialize()
        self.assertEqual(self.client.get('/api/meetings/'+mid).json()['status'],'ready')
        self.assertFalse(list(service.TEMP.glob('meeting-*')))

    @unittest.skipUnless(shutil.which('ffmpeg'),'Requires FFmpeg')
    def test_video_without_audio(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'silent.mp4'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','color=size=160x120:duration=1',str(path)],check=True)
            with self.assertRaises(service.Problem) as exc:asyncio.run(service.probe(path))
            self.assertEqual(exc.exception.code,'NO_AUDIO')

    @unittest.skipUnless(shutil.which('ffmpeg'),'FFmpeg integration runs on Linux server')
    def test_real_media_all_formats_and_duration(self):
        with tempfile.TemporaryDirectory() as directory:
            for ext in ('.mp4','.webm','.mp3','.wav','.m4a'):
                path=Path(directory)/('sample'+ext)
                args=['ffmpeg','-v','error','-f','lavfi','-i','sine=frequency=440:duration=1']
                if ext in ('.mp4','.webm'):
                    args+=['-f','lavfi','-i','color=size=160x120:duration=1','-shortest']
                    args+=['-c:v','libvpx'] if ext=='.webm' else ['-c:v','mpeg4']
                subprocess.run(args+[str(path)],check=True)
                self.assertGreater(asyncio.run(service.probe(path)),0)
            long=Path(directory)/'long.wav'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','sine=duration=301','-ar','8000',str(long)],check=True)
            with self.assertRaises(service.Problem):asyncio.run(service.probe(long))
            boundary=Path(directory)/'boundary.wav'
            subprocess.run(['ffmpeg','-v','error','-f','lavfi','-i','sine=duration=300','-ar','8000',str(boundary)],check=True)
            self.assertEqual(asyncio.run(service.probe(boundary)),300)
            broken=Path(directory)/'broken.mp4';broken.write_bytes(b'not a video')
            with self.assertRaises(service.Problem):asyncio.run(service.probe(broken))


if __name__=='__main__':unittest.main(verbosity=2)
