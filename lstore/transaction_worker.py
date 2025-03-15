from lstore.table import Table, Record
from lstore.index import Index
import threading
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TransactionWorker")

class TransactionWorker:
    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions.copy() if transactions else []
        self.result = 0
        self.thread = None
        self.lock = threading.RLock()  # Use RLock for reentrant locking
        self.is_running = False
        self.is_complete = False
        
    """
    Appends t to transactions
    """
    def add_transaction(self, t):
        with self.lock:
            self.transactions.append(t)
        
    """
    Runs all transaction as a thread
    """
    def run(self):
        with self.lock:
            if self.is_running:
                logger.warning("Worker already running")
                return
                
            self.is_running = True
            self.is_complete = False
            
        self.thread = threading.Thread(target=self.__run)
        self.thread.daemon = True  # Make thread a daemon so it doesn't block program exit
        try:
            self.thread.start()
            logger.info(f"Started transaction worker thread {self.thread.name}")
        except Exception as e:
            with self.lock:
                self.is_running = False
            logger.error(f"Error starting thread: {e}")
    
    """
    Waits for the worker to finish
    """
    def join(self):
        with self.lock:
            if not self.is_running or not self.thread:
                logger.warning("No active thread to join")
                return
                
        try:
            if self.thread and self.thread.is_alive():
                self.thread.join()
                logger.info(f"Thread {self.thread.name} joined successfully")
            else:
                logger.warning("Thread already completed")
        except Exception as e:
            logger.error(f"Error joining thread: {e}")
        finally:
            with self.lock:
                self.is_running = False

    def __run(self):
        try:
            with self.lock:
                local_transactions = self.transactions.copy()
            
            results = []
            for transaction in local_transactions:
                try:
                    # each transaction returns True if committed or False if aborted
                    result = transaction.run()
                    results.append(result)
                except Exception as e:
                    logger.error(f"Error executing transaction: {e}")
                    results.append(False)
            
            with self.lock:
                self.stats = results
                # stores the number of transactions that committed
                self.result = len(list(filter(lambda x: x, results)))
                self.is_complete = True
                self.is_running = False
                
            logger.info(f"Worker completed with {self.result} successful transactions out of {len(results)}")
        except Exception as e:
            with self.lock:
                self.is_complete = False
                self.is_running = False
            logger.error(f"Unexpected error in transaction worker thread: {e}")
    
    def is_done(self):
        with self.lock:
            return self.is_complete

