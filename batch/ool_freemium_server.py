import subprocess
import time

def run_ool():
    # Start the processes in the right order. Starting a process sets up the queue; a queue-name MUST exist before
    # a message is sent to this queue.
    time.sleep(30)
    subprocess.Popen(['server.cmd'])

if __name__ == '__main__':
    run_ool()