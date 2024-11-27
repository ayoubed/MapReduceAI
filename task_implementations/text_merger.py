from typing import Dict, Any
from task import Task

class TextMergerTask(Task):
    """Task for merging multiple translation results"""
    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        merged_results = {
            "translations": [],
            "summary": ""
        }
        
        for task_id, result in inputs.items():
            merged_results["translations"].append(result["translated_content"])
            
        merged_results["summary"] = f"Successfully merged {len(merged_results['translations'])} translations"
        
        return merged_results