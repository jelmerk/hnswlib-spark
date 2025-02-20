package com.github.jelmerk

/** Classes extending this trait need to be manually disposed.
  */
trait Disposable {

  /** Dispose of this class and release all resources it holds.
    */
  def dispose(): Unit

}
