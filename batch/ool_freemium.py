import subprocess
import time

def run_ool():
    # Start the processes in the right order. Starting a process sets up the queue; a queue-name MUST exist before
    # a message is sent to this queue.
    subprocess.Popen(['server.cmd'])
    time.sleep(2)
    subprocess.Popen(['kpn_ool_order_entry.cmd'])
    time.sleep(2)
    subprocess.Popen(['kpn_ool_mail.cmd'])
    time.sleep(2)
    subprocess.Popen(['kpn_ool_timer.cmd'])

if __name__ == '__main__':
    run_ool()