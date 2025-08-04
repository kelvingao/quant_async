#!/usr/bin/env python3
"""
Coverage Badge Generator for Quant Async

This script generates a coverage badge and updates the README.md file.
It can be run manually or as part of a CI/CD pipeline.
"""

import json
import os
import sys
import urllib.parse
from pathlib import Path


def get_coverage_percentage():
    """Extract coverage percentage from coverage.json file."""
    coverage_file = Path("coverage.json")
    
    if not coverage_file.exists():
        print("❌ coverage.json not found. Run 'uv run pytest --cov=src/quant_async --cov-report=json' first.")
        return None
    
    try:
        with open(coverage_file, 'r') as f:
            data = json.load(f)
        
        total_coverage = data.get('totals', {}).get('percent_covered', 0)
        return round(total_coverage)
        
    except (json.JSONDecodeError, KeyError) as e:
        print(f"❌ Error reading coverage.json: {e}")
        return None


def get_coverage_color(coverage):
    """Get color for coverage badge based on percentage."""
    if coverage >= 90:
        return "brightgreen"
    elif coverage >= 80:
        return "green"
    elif coverage >= 70:
        return "yellowgreen"
    elif coverage >= 60:
        return "yellow"
    elif coverage >= 50:
        return "orange"
    else:
        return "red"


def generate_badge_url(coverage):
    """Generate shields.io badge URL for coverage."""
    color = get_coverage_color(coverage)
    # Encode the URL properly
    label = urllib.parse.quote("coverage")
    message = urllib.parse.quote(f"{coverage}%")
    
    return f"https://img.shields.io/badge/{label}-{message}-{color}"


def update_readme_badge(coverage):
    """Update or add coverage badge to README.md."""
    readme_file = Path("README.md")
    
    if not readme_file.exists():
        print("❌ README.md not found")
        return False
    
    # Read current README content
    with open(readme_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Create badge markdown
    badge_url = generate_badge_url(coverage)
    badge_markdown = f"![Coverage]({badge_url})"
    
    # Look for existing coverage badge and replace it
    lines = content.split('\n')
    badge_updated = False
    
    for i, line in enumerate(lines):
        # Look for existing coverage badge
        if "![Coverage]" in line and "shields.io" in line and "coverage" in line:
            lines[i] = badge_markdown
            badge_updated = True
            break
        # Or look for a line with other badges to add coverage badge
        elif line.strip().startswith("![") and ("shields.io" in line or "badge" in line):
            # Add coverage badge after existing badges
            lines.insert(i + 1, badge_markdown)
            badge_updated = True
            break
    
    # If no badge found, add it after the main heading
    if not badge_updated:
        for i, line in enumerate(lines):
            if line.startswith("# ") and i < 10:  # Main heading, within first 10 lines
                # Add badge after the heading, with empty lines
                lines.insert(i + 2, "")
                lines.insert(i + 3, badge_markdown)
                lines.insert(i + 4, "")
                badge_updated = True
                break
    
    if not badge_updated:
        print("⚠️ Could not find appropriate place to add coverage badge")
        return False
    
    # Write updated content back to README
    with open(readme_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    
    print(f"✅ Coverage badge updated: {coverage}% ({get_coverage_color(coverage)})")
    return True


def main():
    """Main function to generate and update coverage badge."""
    print("🎯 Generating coverage badge...")
    
    # Change to script directory to ensure correct paths
    script_dir = Path(__file__).parent.parent
    os.chdir(script_dir)
    
    # Get coverage percentage
    coverage = get_coverage_percentage()
    if coverage is None:
        sys.exit(1)
    
    print(f"📊 Current coverage: {coverage}%")
    
    # Update README with badge
    if update_readme_badge(coverage):
        print("🎉 Coverage badge successfully updated in README.md!")
        print(f"🔗 Badge URL: {generate_badge_url(coverage)}")
    else:
        print("❌ Failed to update coverage badge")
        sys.exit(1)


if __name__ == "__main__":
    main()