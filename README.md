# tuck
A lightweight boilerplate library implementing a commonly used key-value store locally.

## Installation
pip install tuck

## Usage
from tuck import Tuck
store = Tuck()
store.set("key", {"value": "data"})
print(store.get("key"))  # {"value": "data"}

