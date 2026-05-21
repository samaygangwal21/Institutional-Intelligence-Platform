"""Run app.main in a subprocess and capture the error."""
import sys, subprocess, os
sys.stdout.reconfigure(encoding='utf-8')

result = subprocess.run(
    [sys.executable, '-c', 'import sys; sys.path.insert(0,"."); import app.main'],
    capture_output=True, text=True, timeout=30,
    cwd=os.path.dirname(os.path.abspath(__file__))
)
print('STDOUT:', result.stdout[:3000])
print('STDERR:', result.stderr[:3000])
print('Exit code:', result.returncode)
