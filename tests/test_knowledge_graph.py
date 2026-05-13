from shared.knowledge_graph import build_candidate_graph, render_candidate_graph


def test_knowledge_graph_compacts_candidate_profile():
    graph = build_candidate_graph(
        "Teja Gopal Reddy Vaka Senior AI Engineer. Skills: Python, PyTorch, AWS, "
        "LangChain, RAG, LLMs, vector databases. Achievements: 33% GPU cost reduction, "
        "87% autonomous ticket resolution. Projects: RetireIQ and Rivulet. Open to relocate "
        "and onsite in South San Francisco."
    )
    rendered = render_candidate_graph(graph)

    assert graph["candidate_name"] == "Teja Gopal Reddy Vaka"
    assert "Python" in graph["skills"]
    assert "RAG pipelines" in graph["focus_areas"]
    assert any(project["name"] == "RetireIQ" for project in graph["projects"])
    assert "<candidate_graph>" in rendered
    assert "preferences:" in rendered
