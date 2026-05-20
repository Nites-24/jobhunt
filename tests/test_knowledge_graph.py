from shared.knowledge_graph import build_candidate_graph, render_candidate_graph


def test_knowledge_graph_compacts_candidate_profile():
    graph = build_candidate_graph(
        "Teja Gopal Reddy Vaka AI Engineer. Skills: Python, PyTorch, AWS, "
        "LangChain, RAG, LLMs, vector databases. Achievements: 33% GPU cost reduction, "
        "87% autonomous ticket resolution. Projects: RetireIQ and Rivulet. Currently in "
        "Wichita, Kansas. Open to relocate and open to remote, hybrid, or onsite roles."
    )
    rendered = render_candidate_graph(graph)

    assert graph["candidate_name"] == "Teja Gopal Reddy Vaka"
    assert "Python" in graph["skills"]
    assert "RAG pipelines" in graph["focus_areas"]
    assert any(project["name"] == "RetireIQ" for project in graph["projects"])
    assert "currently in Wichita, Kansas" in graph["preferences"]
    assert "<candidate_graph>" in rendered
    assert "preferences:" in rendered
