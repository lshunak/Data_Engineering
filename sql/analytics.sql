CREATE OR REPLACE VIEW valid_courses AS
SELECT DISTINCT code_module, code_presentation
FROM assessments a1
WHERE EXISTS (
    SELECT 1 
    FROM assessments a2 
    WHERE a2.code_module = a1.code_module 
    AND a2.code_presentation = a1.code_presentation
    AND a2.assessment_type = 'exam'
)
AND EXISTS (
    SELECT 1 
    FROM assessments a3 
    WHERE a3.code_module = a1.code_module 
    AND a3.code_presentation = a1.code_presentation
    AND a3.assessment_type != 'exam'
);

CREATE OR REPLACE VIEW course_scores AS
SELECT 
	vc.code_module,
    vc.code_presentation,
    sa.id_student,
    SUM(CASE 
        WHEN a.assessment_type != 'Exam' 
        THEN sa.score * IFNULL(a.weight, 1)
    END) / SUM(CASE 
        WHEN a.assessment_type != 'Exam' 
        THEN IFNULL(a.weight, 1)
    END) as weighted_score,
    MAX(CASE 
		WHEN a.assessment_type = 'exam' 
		THEN sa.score 
	END) as exam_score
    FROM valid_courses vc
    JOIN assessments a 
		ON vc.code_module = a.code_module
        AND vc.code_presentation = a.code_presentation
	JOIN studentAssessment sa
		ON sa.id_assessment = a.id_assessment
	WHERE sa.score IS NOT NULL
    GROUP BY
		vc.code_module,
		vc.code_presentation,
		sa.id_student
	HAVING weighted_score IS NOT NULL
    AND exam_score IS NOT NULL;

SELECT * FROM course_scores;

SELECT 
    code_module,
    code_presentation,
    ROUND(AVG(weighted_score), 2) as avg_assessment_score,
    ROUND(AVG(exam_score), 2) as avg_exam_score,
    ROUND(
        (AVG(weighted_assessment_score * exam_score) - AVG(weighted_assessment_score) * AVG(exam_score)) /
        (STDDEV(weighted_assessment_score) * STDDEV(exam_score)), 
        3
    ) as correlation
FROM course_scores
GROUP BY code_module, code_presentation
ORDER BY code_module, code_presentation;