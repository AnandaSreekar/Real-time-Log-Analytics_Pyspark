"""
Convenience Runner for the PySpark Real-Time Log Analytics Pipeline.

Can launch all three services (Log Generator, PySpark Streaming Job, Dashboard Web Server)
concurrently for quick demonstration, or run individual components.
"""
import sys
import subprocess
import time
import argparse
import os
import signal

PYTHON_EXE = sys.executable
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def start_all():
    print("=" * 70)
    print("  LAUNCHING PYSPARK REAL-TIME LOG ANALYTICS SUITE")
    print("=" * 70)
    print("1. Dashboard Web Server -> http://127.0.0.1:5000")
    print("2. PySpark Structured Streaming Job (Background Engine)")
    print("3. Synthetic Log Generator (Live Traffic Simulator)")
    print("=" * 70)
    print("Press Ctrl+C at any time to gracefully terminate all services.\n")

    processes = []
    try:
        # 1. Start Dashboard Server
        print("[*] Starting Dashboard Web Server...")
        p_dash = subprocess.Popen([PYTHON_EXE, os.path.join(BASE_DIR, "dashboard_server.py")])
        processes.append(("Dashboard Server", p_dash))
        time.sleep(1)

        # 2. Start PySpark Streaming Pipeline
        print("[*] Starting PySpark Structured Streaming Pipeline...")
        p_spark = subprocess.Popen([PYTHON_EXE, os.path.join(BASE_DIR, "streaming_job.py")])
        processes.append(("PySpark Streaming", p_spark))
        time.sleep(2)

        # 3. Start Synthetic Generator
        print("[*] Starting Synthetic Log Generator...")
        p_gen = subprocess.Popen([PYTHON_EXE, os.path.join(BASE_DIR, "generator.py")])
        processes.append(("Log Generator", p_gen))

        print("\n[+] All services running! Open http://127.0.0.1:5000 in your browser.")
        print("[+] Watching live terminal logs below...\n")

        # Monitor processes
        while True:
            for name, proc in processes:
                ret = proc.poll()
                if ret is not None:
                    print(f"\n[!] Process {name} exited with return code {ret}.")
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[*] Gracefully shutting down all processes...")
        for name, proc in processes:
            try:
                proc.terminate()
                proc.wait(timeout=5)
                print(f"[+] Terminated {name}")
            except Exception:
                proc.kill()
        print("[+] All services stopped.")


def main():
    parser = argparse.ArgumentParser(description="PySpark Analytics Suite Runner")
    parser.add_argument(
        "--service",
        choices=["all", "generator", "spark", "dashboard"],
        default="all",
        help="Choose which service to run (default: all)"
    )
    args = parser.parse_args()

    if args.service == "all":
        start_all()
    elif args.service == "generator":
        subprocess.run([PYTHON_EXE, os.path.join(BASE_DIR, "generator.py")])
    elif args.service == "spark":
        subprocess.run([PYTHON_EXE, os.path.join(BASE_DIR, "streaming_job.py")])
    elif args.service == "dashboard":
        subprocess.run([PYTHON_EXE, os.path.join(BASE_DIR, "dashboard_server.py")])


if __name__ == "__main__":
    main()
