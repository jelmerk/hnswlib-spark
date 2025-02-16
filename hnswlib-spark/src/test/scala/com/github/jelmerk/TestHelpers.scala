package com.github.jelmerk

import java.io.File
import java.nio.file.Files
import java.util.UUID

import org.apache.commons.io.FileUtils

object TestHelpers extends TestHelpers

trait TestHelpers {

  def withTempFolder[T](fn: File => T): T = {
    val tempDir = Files.createTempDirectory(UUID.randomUUID().toString).toFile
    try {
      fn(tempDir)
    } finally {
      FileUtils.deleteDirectory(tempDir)
    }
  }

  def withDisposableResource[T <: Disposable, R](model: T)(fn: T => R): R = {
    try {
      fn(model)
    } finally {
      model.dispose()
    }
  }
}
