package ibd.table;

import java.util.Arrays;

public class mainBruna {

  public mainBruna() { }

  public void addAluno(Table table, long pk, String nome, int matricula) throws Exception {
    String json = buildAluno(nome, matricula);

    table.addRecord(pk, json);
    table.flushDB();
  }

  public String buildAluno(String nome, int matricula) {
    return "{\"nome\": \"" + nome + "\", \"matricula\": " + matricula + "}";
  }

  public String getNome(String aluno) {
    String[] tmp = aluno.split(",");
    return Arrays.toString(tmp[0].split(":"));
  }

  public String getMatricula(String aluno){
    String[] tmp = aluno.split(",");
    return Arrays.toString(tmp[1].split(":")).replace("}", "");
  }

}