"""Explicit research routing, not automatic classification or price prediction."""
COMMON = [
    "איזה מקור ראשוני מאשר את האירוע, זמן הפרסום ומועד הכניסה לתוקף?",
    "מה היה צפוי לפני הפרסום, ומה חדש ביחס לציפיות?",
    "מהו ערוץ ההשפעה על ההכנסות, העלויות, המימון או הביקוש בכל גורם?",
    "מה סותר את ההשערה, ואיזו תצפית תפריך אותה?",
    "האם החשיפה מהותית, האם קיים מכשיר סחיר, והאם ההשפעה כבר מגולמת במחיר שניתן לבצע בו עסקה?",
]
ROUTES = {
    "earnings": ["הפרד תוצאות, תחזית והשפעות חשבונאיות; השווה לתחזית השוק באותה הגדרת מדד."],
    "merger": ["בדוק תנאים מחייבים, מימון, הגבלים עסקיים, תנאי השלמה והצעות מתחרות."],
    "financing": ["כמת דילול, עלות מימון, שימוש בכסף וסיכון מיחזור חוב."],
    "investment_contract": ["האם ההוצאה מחייבת ונוספת, מי חתום על החוזה ומתי יוכרו ההכנסות?"],
    "monetary_policy": ["הפרד הפתעת ריבית מהתוואי הצפוי; בדוק מח״מ, תמחור מחדש של חוב וחשיפת מטבע."],
    "macro_release": ["בדוק תיקונים, ניכוי עונתיות, מועד תחזית השוק וערוצי צמיחה ואינפלציה מנוגדים."],
    "trade_sanctions": ["בדוק תחולה, חריגים, מועד אכיפה, הכנסות לפי אזור ואפשרויות רכש חלופיות."],
    "supply_disruption": ["בדוק מלאי, תחליפים, ביטוח, לוגיסטיקה, משך ההשבתה ויכולת לגלגל עלויות."],
    "technology": ["בדוק אימות טכני, עלויות אימוץ, יכולת הרחבה, מתחרים וסיכון החלפה."],
    "regulatory_clinical": ["הפרד תוצאות ניסוי מאישור; בדוק אוכלוסייה, בטיחות, תנאים וזכויות מסחור."],
    "management_statement": ["הפרד כוונה מהתחייבות חתומה; אמת את הדובר ואת ההקשר המלא."],
    "market_structure": ["בדוק נזילות, מועדי שינוי מדדים, פוזיציות וזרימה זמנית מול ביקוש מתמשך."],
    "unknown": ["סווג את האירוע לפי ראיות מאומתות; אין להסיק עסקה ממילות מפתח."],
}
COUNTERARGUMENTS = [
    "The event may be anticipated, conditional, revised or incorrectly attributed.",
    "Opposing channels, hedges, substitutes or implementation delays may offset the effect.",
    "An economic benefit does not establish a profitable trade after price reaction and costs.",
]
def research_brief(event_types=None):
    types = ["unknown"] if event_types is None else event_types
    if not isinstance(types, (list, tuple)) or not types or any(not isinstance(t,str) or t not in ROUTES for t in types):
        raise ValueError("Explicit known event types required")
    types = list(dict.fromkeys(types))
    return {"event_types": types, "classification_verified": False,
            "questions": COMMON + [q for t in types for q in ROUTES[t]],
            "counterarguments": list(COUNTERARGUMENTS), "order_permission": "none"}
