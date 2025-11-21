import random
import csv
import os

# Configuration
OUTPUT_FILE = "../../data/trade.csv"  # Relative path to data folder
NUM_TRADES = 10_000_000               # 10 Million rows
NUM_SYMBOLS_TARGET = 100_000          # Target size for the symbol pool
FRACTAL_PROB = 0.3                    # 70-30 rule (0.3 frac)
INITIAL_N = 70_000                    # Starting N (calculated as 100k * 0.7)

# Price Configuration
MIN_PRICE = 50
MAX_PRICE = 500
MIN_QTY = 100
MAX_QTY = 10000
PRICE_DELTA_MIN = 1
PRICE_DELTA_MAX = 5

def gen_fractal_symbols(frac, n):
    """
    Generates a list of stock symbols based on the fractal distribution 
    pseudocode provided in the assignment.
    """
    print(f"Generating symbol pool with N={n}, frac={frac}...")
    
    # p: random permutation of numbers from 1 to N
    p = [f"SYM{i}" for i in range(1, n + 1)]
    random.shuffle(p)
    
    outvec = list(p) # Make a copy
    
    current_p = p
    
    # Recursively prepend the top fraction until p has 1 element or less
    while len(current_p) > 1:
        split_idx = int(frac * len(current_p))
        
        # Avoid infinite loops if split_idx becomes 0 but len > 1
        if split_idx == 0:
            break
            
        # p: first frac*|p| elements of p
        current_p = current_p[:split_idx]
        
        # prepend p to outvec
        outvec = current_p + outvec
        
    print(f"Final symbol pool size: {len(outvec)}")
    random.shuffle(outvec)
    return outvec

def generate_trades():
    # 1. Generate the pool of symbols (Fractal Distribution)
    symbol_pool = gen_fractal_symbols(FRACTAL_PROB, INITIAL_N)
    pool_size = len(symbol_pool)

    # 2. Initialize prices for all unique symbols
    # We need a set of unique symbols to track their current price state
    unique_symbols = set(symbol_pool)
    # Map: Symbol -> Current Price (Start randomly between 50-500)
    current_prices = {sym: random.randint(MIN_PRICE, MAX_PRICE) for sym in unique_symbols}

    print(f"Generating {NUM_TRADES} trades...")
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

    with open(OUTPUT_FILE, 'w', newline='') as f:
        writer = csv.writer(f)

        writer.writerow(['stocksymbol', 'time', 'quantity', 'price'])

        for t in range(1, NUM_TRADES + 1):
            # Choose symbol uniformly from the fractal pool
            sym = symbol_pool[random.randint(0, pool_size - 1)]
            
            # Get last price
            last_price = current_prices[sym]
            
            # Calculate new price
            # vary by at least 1 but no more than 5
            delta = random.randint(PRICE_DELTA_MIN, PRICE_DELTA_MAX)
            
            # Randomly decide up or down
            if random.random() < 0.5:
                delta = -delta
            
            new_price = last_price + delta
            
            # should still stay within that interval [50-500]
            if new_price < MIN_PRICE:
                new_price = MIN_PRICE + abs(delta) # Bounce back up
            elif new_price > MAX_PRICE:
                new_price = MAX_PRICE - abs(delta) # Bounce back down
            
            # Update state
            current_prices[sym] = new_price
            
            # Quantity 100-10000
            qty = random.randint(MIN_QTY, MAX_QTY)
            
            # Write row
            writer.writerow([sym, t, qty, new_price])
            
            if t % 1_000_000 == 0:
                print(f"Generated {t} trades...")

    print(f"Done! Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_trades()