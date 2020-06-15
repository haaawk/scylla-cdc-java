package com.scylladb.scylla.cdc.printer;

import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.scylladb.cdc.ScyllaCDC;
import com.scylladb.cdc.common.FutureUtils;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Main {

  private static void printChanges(String source, String keyspace, String table)
      throws InterruptedException, ExecutionException {
    try (Cluster cluster = Cluster.builder().addContactPoints(source).build(); Session session = cluster.connect()) {
      ScyllaCDC cdc = new ScyllaCDC(session, keyspace, table, c -> {
        System.out.println(c);
        return FutureUtils.completed(null);
      });

      cdc.fetchChanges().get();
    }
  }

  private static Namespace parseArguments(String[] args) {
    ArgumentParser parser = ArgumentParsers.newFor("CDCPrinter").build().defaultHelp(true);
    parser.addArgument("-k", "--keyspace").help("Keyspace name");
    parser.addArgument("-t", "--table").help("Table name");
    parser.addArgument("-s", "--source").help("Address of a node in source cluster");

    try {
      return parser.parseArgs(args);
    } catch (ArgumentParserException e) {
      parser.handleError(e);
      System.exit(-1);
      return null;
    }
  }

  public static void main(String[] args) throws Exception {
    Namespace ns = parseArguments(args);
    printChanges(ns.getString("source"), ns.getString("keyspace"), ns.getString("table"));
  }

}
