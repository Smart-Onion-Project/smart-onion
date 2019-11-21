import { CategoryInfo } from "../models/category-info/category-info.model";
import { Http } from "@angular/http";
import { Injectable } from "@angular/core";
import { map } from 'rxjs/operators';

@Injectable()
export class BackendService {
  private backendBaseUrl = "http://localhost:8080";

  constructor(private httpClient : Http) {}

  //[
  //   {"category_id": "dfgdf-324532-43654546", "category_name": "blah blah"},
  //   {"category_id": "dfgdf-324532-43654546", "category_name": "blah blah"}
  //]
  getAllCategories() {
    var res : CategoryInfo[] = [];
    var rawBackendResponse : { category_id : string, category_name : string }[] = [];

    this.httpClient.get(this.backendBaseUrl + "/get-all-categories")
    .pipe(map(responseData => {
      console.log(responseData);
      // response.json().categories.map((categoryRawInfo : { category_id : string, category_name : string }) => {
      //   res.push(new CategoryInfo(categoryRawInfo.category_id, categoryRawInfo.category_name));
      // });

      return res;
    }))
    .subscribe(response => {
    }, error => {
      throw error;
    }, () => {
      return res;
    });

    return [new CategoryInfo("dfgdf-324532-43654546", "blah blah")];
  }

  getAllQueryIds() {

  }

  getRandomQuestion() {

  }

  getGrade() {

  }

  reportQuery() {

  }

  addNewCategory() {

  }
}