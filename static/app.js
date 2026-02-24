fetch("/dados")
  .then(res => res.json())
  .then(dados => {
    const container = document.getElementById("cards");
    container.innerHTML = "";

    dados.forEach((item, index) => {
      const card = document.createElement("div");
      card.className = "card";

      // Título do card (prioridade para Código do Anúncio)
      const titulo = item["Código do Anúncio"] || item["Código"] || `Item ${index + 1}`;
      card.innerHTML = `<h3>🧾 ${titulo}</h3>`;

      // Criar um campo para CADA coluna
      Object.keys(item).forEach(chave => {
        const valor = item[chave];

        const field = document.createElement("div");
        field.className = "field";

        field.innerHTML = `
          <strong>${chave}</strong>
          <span>${valor === null || valor === "" ? "-" : valor}</span>
        `;

        card.appendChild(field);
      });

      container.appendChild(card);
    });
  })
  .catch(err => {
    console.error("Erro ao carregar dados:", err);
  });

