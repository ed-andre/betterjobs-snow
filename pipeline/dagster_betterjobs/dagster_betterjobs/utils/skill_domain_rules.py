"""
Domain-Specific Business Rules for Skill Consolidation

Configuration and rules for handling special cases in skill consolidation,
including protected terms and domain-specific preferences.

For ENHANCEMENT-023 Intelligent Skills Variant Consolidation
"""

from typing import Dict, List, Set
from dataclasses import dataclass


@dataclass
class DomainRuleSet:
    """Configuration for domain-specific consolidation rules."""
    protected_terms: Set[str]
    preferred_forms: Dict[str, str]
    category_rules: Dict[str, Dict[str, str]]
    compound_patterns: Dict[str, str]


# Medical/Scientific terms that should never be pluralized
MEDICAL_SCIENTIFIC_PROTECTED = {
    "Anesthesia", "Analysis", "Business", "Physics", "Mathematics",
    "Statistics", "Economics", "Logistics", "Graphics", "Analytics",
    "Dynamics", "Genetics", "Robotics", "Acoustics", "Optics",
    "Bioinformatics", "Informatics", "Forensics", "Linguistics",
    "Phonetics", "Semantics", "Pragmatics", "Kinetics", "Thermodynamics",
    "Biomechanics", "Ergonomics", "Pneumatics", "Hydraulics"
}

# Technology-specific preferred forms
TECHNOLOGY_PREFERRED_FORMS = {
    # APIs and Web Technologies
    "apis": "API",
    "rest apis": "REST API",
    "graphql apis": "GraphQL API",
    "web apis": "Web API",

    # AI and Machine Learning
    "ai tools": "AI Tool",
    "ml models": "ML Model",
    "neural networks": "Neural Network",
    "deep learning models": "Deep Learning Model",
    "machine learning algorithms": "Machine Learning Algorithm",

    # Web Development
    "web frameworks": "Web Framework",
    "javascript frameworks": "JavaScript Framework",
    "css frameworks": "CSS Framework",
    "frontend frameworks": "Frontend Framework",
    "backend frameworks": "Backend Framework",

    # Mobile Development
    "mobile apps": "Mobile App",
    "android apps": "Android App",
    "ios apps": "iOS App",
    "mobile frameworks": "Mobile Framework",

    # Databases
    "databases": "Database",
    "relational databases": "Relational Database",
    "nosql databases": "NoSQL Database",
    "graph databases": "Graph Database",
    "time series databases": "Time Series Database",

    # Cloud and Infrastructure
    "cloud platforms": "Cloud Platform",
    "cloud services": "Cloud Service",
    "microservices": "Microservice",
    "web services": "Web Service",
    "containers": "Container",
    "orchestration tools": "Orchestration Tool",

    # Development Tools
    "development tools": "Development Tool",
    "testing frameworks": "Testing Framework",
    "build tools": "Build Tool",
    "deployment tools": "Deployment Tool",
    "monitoring tools": "Monitoring Tool",
    "debugging tools": "Debugging Tool",

    # Programming Languages
    "programming languages": "Programming Language",
    "scripting languages": "Scripting Language",
    "markup languages": "Markup Language",
    "query languages": "Query Language",

    # Operating Systems
    "operating systems": "Operating System",
    "linux distributions": "Linux Distribution",
    "virtualization platforms": "Virtualization Platform",

    # Data Science
    "data visualization tools": "Data Visualization Tool",
    "analytics platforms": "Analytics Platform",
    "business intelligence tools": "Business Intelligence Tool",
    "etl tools": "ETL Tool",

    # Security
    "security tools": "Security Tool",
    "encryption algorithms": "Encryption Algorithm",
    "authentication protocols": "Authentication Protocol",
    "security frameworks": "Security Framework"
}

# Business and methodology preferred forms
BUSINESS_PREFERRED_FORMS = {
    "methodologies": "Methodology",
    "agile methodologies": "Agile Methodology",
    "project management methodologies": "Project Management Methodology",
    "software development methodologies": "Software Development Methodology",

    "strategies": "Strategy",
    "business strategies": "Business Strategy",
    "marketing strategies": "Marketing Strategy",
    "testing strategies": "Testing Strategy",

    "processes": "Process",
    "business processes": "Business Process",
    "development processes": "Development Process",
    "quality assurance processes": "Quality Assurance Process",

    "procedures": "Procedure",
    "protocols": "Protocol",
    "standards": "Standard",
    "practices": "Practice",
    "best practices": "Best Practice",
    "coding practices": "Coding Practice",

    "principles": "Principle",
    "design principles": "Design Principle",
    "software engineering principles": "Software Engineering Principle",

    "concepts": "Concept",
    "programming concepts": "Programming Concept",
    "architectural concepts": "Architectural Concept",

    "techniques": "Technique",
    "optimization techniques": "Optimization Technique",
    "testing techniques": "Testing Technique",
    "debugging techniques": "Debugging Technique",

    "approaches": "Approach",
    "solutions": "Solution",
    "platforms": "Platform",
    "systems": "System",
    "applications": "Application",
    "technologies": "Technology",
    "environments": "Environment",
    "architectures": "Architecture"
}

# Category-specific consolidation rules
CATEGORY_RULES = {
    "languages": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Language"
    },
    "frameworks": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Framework"
    },
    "databases": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Database"
    },
    "tools": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Tool"
    },
    "platforms": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Platform"
    },
    "services": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Service"
    },
    "protocols": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Protocol"
    },
    "methodologies": {
        "preferred_form": "singular",
        "compound_pattern": "{adjective} Methodology"
    }
}

# Compound phrase patterns for technology terms
COMPOUND_PATTERNS = {
    # Pattern: "adjective + noun" -> preferred singular form
    "web development": "Web Development",
    "software development": "Software Development",
    "mobile development": "Mobile Development",
    "frontend development": "Frontend Development",
    "backend development": "Backend Development",
    "full stack development": "Full Stack Development",

    "data analysis": "Data Analysis",
    "business analysis": "Business Analysis",
    "system analysis": "System Analysis",
    "requirements analysis": "Requirements Analysis",

    "project management": "Project Management",
    "product management": "Product Management",
    "risk management": "Risk Management",
    "change management": "Change Management",

    "quality assurance": "Quality Assurance",
    "software testing": "Software Testing",
    "user experience": "User Experience",
    "user interface": "User Interface",

    "machine learning": "Machine Learning",
    "deep learning": "Deep Learning",
    "artificial intelligence": "Artificial Intelligence",
    "computer vision": "Computer Vision",

    "cloud computing": "Cloud Computing",
    "distributed computing": "Distributed Computing",
    "parallel computing": "Parallel Computing",
    "edge computing": "Edge Computing"
}


def get_default_domain_rules() -> DomainRuleSet:
    """
    Get the default domain-specific rules for skill consolidation.

    Returns:
        DomainRuleSet with all configured rules
    """
    # Combine all preferred forms
    all_preferred_forms = {}
    all_preferred_forms.update({k.lower(): v for k, v in TECHNOLOGY_PREFERRED_FORMS.items()})
    all_preferred_forms.update({k.lower(): v for k, v in BUSINESS_PREFERRED_FORMS.items()})
    all_preferred_forms.update({k.lower(): v for k, v in COMPOUND_PATTERNS.items()})

    return DomainRuleSet(
        protected_terms=MEDICAL_SCIENTIFIC_PROTECTED,
        preferred_forms=all_preferred_forms,
        category_rules=CATEGORY_RULES,
        compound_patterns=COMPOUND_PATTERNS
    )


def is_protected_term(term: str, rules: DomainRuleSet = None) -> bool:
    """
    Check if a term is protected from pluralization changes.

    Args:
        term: The skill term to check
        rules: Domain rules to use (defaults to default rules)

    Returns:
        True if the term should be protected from changes
    """
    if rules is None:
        rules = get_default_domain_rules()

    # Check exact matches first
    if term in rules.protected_terms:
        return True

    # Check case-insensitive matches
    term_lower = term.lower()
    for protected in rules.protected_terms:
        if term_lower == protected.lower():
            return True

    return False


def get_preferred_form(term: str, rules: DomainRuleSet = None) -> str:
    """
    Get the preferred canonical form for a skill term.

    Args:
        term: The skill term to get preferred form for
        rules: Domain rules to use (defaults to default rules)

    Returns:
        Preferred form if found, otherwise original term
    """
    if rules is None:
        rules = get_default_domain_rules()

    term_lower = term.lower().strip()

    # Check direct lookup first
    if term_lower in rules.preferred_forms:
        return rules.preferred_forms[term_lower]

    # Check compound patterns
    for pattern, preferred in rules.compound_patterns.items():
        if pattern.lower() in term_lower:
            return preferred

    return term


def get_category_preference(category: str, rules: DomainRuleSet = None) -> str:
    """
    Get the preferred form setting for a skill category.

    Args:
        category: The skill category
        rules: Domain rules to use (defaults to default rules)

    Returns:
        Preferred form setting ("singular", "plural", "most_frequent")
    """
    if rules is None:
        rules = get_default_domain_rules()

    category_lower = category.lower()

    if category_lower in rules.category_rules:
        return rules.category_rules[category_lower].get("preferred_form", "singular")

    # Default preference
    return "singular"


def validate_consolidation_result(original: str, consolidated: str, rules: DomainRuleSet = None) -> bool:
    """
    Validate that a consolidation result follows domain rules.

    Args:
        original: Original skill name
        consolidated: Proposed consolidated name
        rules: Domain rules to use (defaults to default rules)

    Returns:
        True if consolidation result is valid according to domain rules
    """
    if rules is None:
        rules = get_default_domain_rules()

    # Protected terms should not be changed
    if is_protected_term(original, rules) and original != consolidated:
        return False

    # Check if preferred form is being used when available
    preferred = get_preferred_form(original, rules)
    if preferred != original and consolidated != preferred:
        # Log warning but don't fail validation
        pass

    return True