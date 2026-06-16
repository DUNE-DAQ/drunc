"""
Manual overrides for conffwk stubs.

The pybind11-stubgen auto-generator marks types it can't understand as 'Any' 
or misses fields entirely. This adds those classes manually.
"""

import os
import re
from pathlib import Path


def process_file(filepath: Path) -> None:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Fix parameter types
    content = re.sub(r': \.\.\.', ': object', content)

    # Fix return types
    content = re.sub(r'-> \.\.\.:', '-> object:', content)

    # Fix untyped arguments
    content = re.sub(r'= \.\.\.', '= object()', content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


def add_dal_classes(stubs_dir: Path) -> None:
    dal_file = stubs_dir / "dal.pyi"
        
    dal_classes = """
from typing import List

from drunc.fsm._protocols import (
    ConfigurationProtocol,
    DBProtocol,
    OksKeyProtocol,
    ParameterProtocol,
)

class FSMParameter(ParameterProtocol):
    name: str
    value: str

class FSMAction(ConfigurationProtocol):
    id: str
    name: str
    parameters: List[FSMParameter]
    db: DBProtocol
    oks_key: OksKeyProtocol
    initial_data: str

class FSMxTransition:
    transition: str
    order: List[str]
    mandatory: List[str]

class FSMTransitionConfig:
    id: str
    source: str
    dest: str

class FSMCommand:
    id: str

class FSMCommandSequence:
    id: str
    sequence: List[FSMCommand]

class FSMData:
    states: List[str]
    initial_state: str
    actions: List[FSMAction]
    transitions: List[FSMTransitionConfig]
    pre_transitions: List[FSMxTransition]
    post_transitions: List[FSMxTransition]
    command_sequences: List[FSMCommandSequence]
"""
    with open(dal_file, 'w', encoding='utf-8') as f:
        f.write(dal_classes)
    print("[+] Done. dal.pyi is now strictly typed.")

def main() -> None:
    stubs_dir = Path("typings/conffwk")
    
    if not stubs_dir.exists():
        print("Typings/conffwk directory not found. Run 'pybind11-stubgen conffwk --output-dir=typings' first.")
        return

    # Fix syntax errors in all generated files
    for root, _, files in os.walk(stubs_dir):
        for file in files:
            if file.endswith('.pyi'):
                process_file(Path(root) / file)
                
    # Add missing classes into the DAL file
    add_dal_classes(stubs_dir)
    

if __name__ == "__main__":
    main()
