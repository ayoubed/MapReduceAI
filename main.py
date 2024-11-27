import logging
from task_scheduler import TaskScheduler
from task_implementations.text_merger import TextMergerTask
from task_implementations.translation import TranslationTask
from task_implementations.text_parser import TextParsingTask

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

if __name__ == "__main__":
    text1 = """
    Un roman incroyable où le personnage principal, figure de anti-héro, est complètement étranger
    à lui-même et au monde qui l'entoure. La narration est captivante car ce dernier nous fait part
    de tout ce qui l'entoure et parvient à donner un ton morne à l'univers qu'il perçoit. Le roman m'a
    évoqué beaucoup de couleurs sombres: je trouve le gris très présent. Et que dire de cette
    mélancolie sous-jacente, qui côtoie un style poétique raffiné. La lecture n'est pas aisée et l'univers
    solitaire et barricadé du personnage est difficile à appréhender. Pourtant, une fois que l'on s'est
    accaparé de cet univers, difficile d'en sortir. L'intrigue évolue certes lentement mais c'est inhérent
    à ce style morne, asthénique voire neurasthénique que le personnage parvient presque à transmettre
    au lecteur. Le dénouement est intéressant, l'injustice (et la justice) sont des fondamentaux dans l'histoire.
    Pour finir, il me parait important de souligner le côté philosophique inhérent à Camus: l'absurdité
    de la vie, de la normalité et de la petite vie tranquille qu'on est sensé s'ériger. La conception
    de la beauté, surfaite, que l'on peut très bien trouver ailleurs que là où elle serait attendue.
    Un roman complet, singulier, une œuvre magistrale, déprimante certes, mais qui fait tellement réfléchir
    sur l'existence et la façon de percevoir la vie. L'étranger peut-être vu comme LE roman défenseur
    de la doctrine existentialiste.
    """
    
    text2 = """
    Dans la première partie, le style de ce roman est télégraphique, avec une épuration des détails,
    et épouse le regard du "héros" (entre "", car il n'est pas vraiment l'archétype du héros) :
    comme lui, nous devenons alors un observateur extérieur au monde, un étranger (d'où le sens du titre).
    L'absence d'émotions, d'interprétation, ou de préjugés (seules ses sensations présentes et l'immédiat l'intéressent)
    du "héros", son aspect lisse et direct (il ne peut pas mentir), est en fait le reflet de la vision
    (pessimiste ?) de l'auteur, qui est celle d'un monde absurde, d'où le sens est a priori absent.
    Spoiler(cliquez pour révéler)
    La deuxième partie du roman est la tentative de donner du sens aux actes du "héros".
    Spoiler(cliquez pour révéler)
    Pour comprendre la portée de ce livre, il faudrait le lire à la lumière de la deuxième guerre mondiale
    (ce livre est écrit en 1942), du christianisme, et du courant de l'existentialisme, proche de la signification
    de l'absurde chez Camus : comme lui, il vise le conventionnalisme moral et social, les habitudes.
    Ce n'est pas forcément un roman qu'on lit pour se faire plaisir (bien qu'il ne soit pas totalement dénué d'humour),
    et je l'ai également beaucoup apprécié pour son point de vue sur l'existence, qui n'est pas totalement négatif,
    mais simplement qui refuse celui qui est donné habituellement par la société, et pour l'ouverture offerte aux
    différentes interprétations. Un véritable diamant aux multiples facettes que je ne conseille pas, encore une fois,
    pour rêver, mais pour réfléchir sur ce que nous acceptons par habitude, par convention.
    Avec en prime une magnifique écriture et un style, une forme, qui participe au contenu du récit.
    PS : je comprends les commentaires des lycéens qui n'ont pas apprécié ce roman,
    ce n'est que maintenant, en faisant une lecture "libre", que je l'ai apprécié !
    """
    
    parse_task1 = TextParsingTask(
        text=text1,
        task_id="parse1",
        timeout=10.0
    )
    
    parse_task2 = TextParsingTask(
        text=text2,
        task_id="parse2",
        timeout=10.0
    )
    
    translate_task1 = TranslationTask(
        target_language="English",
        task_id="translate1",
        timeout=10.0,
        required_dependencies=["parse1"]
    )
    
    translate_task2 = TranslationTask(
        target_language="English",
        task_id="translate2",
        timeout=10.0,
        required_dependencies=["parse2"]
    )
    
    merge_task = TextMergerTask(
        task_id="merge",
        required_dependencies=["translate1", "translate2"]
    )
    
    scheduler = TaskScheduler(default_timeout=10.0)
    
    for task in [parse_task1, parse_task2, translate_task1, translate_task2, merge_task]:
        scheduler.add_task(task)
    
    results = scheduler.execute()
    
    if "merge" in results and not results["merge"].error:
        print("\nFinal Merged Results:")
        merged_data = results["merge"].output
        print("\nTranslations:")
        for i, translation in enumerate(merged_data["translations"], 1):
            print(f"\nTranslation {i}:")
            print(translation)
        print(f"\nSummary: {merged_data['summary']}")
    else:
        print("Error in processing pipeline")