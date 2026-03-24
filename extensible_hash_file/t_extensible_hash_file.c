#include "../Unity/unity.h"
#include "extensible_hash_file.h"

#include <stdio.h>
#include <string.h>

#define TEST_FILE "test_hash.dat"

static ExtHash h;

static void remove_test_file(void)
{
    remove(TEST_FILE);
}

void setUp(void)
{
    h = NULL;
    remove_test_file();
}

void tearDown(void)
{
    if (h != NULL)
    {
        ehf_close(h);
        h = NULL;
    }
    remove_test_file();
}

void test_create_deve_retornar_handle_valido(void)
{
    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);
}

void test_insert_e_search_devem_funcionar(void)
{
    int valor = 42;
    int out = 0;
    size_t out_size = sizeof(out);

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "chave1", &valor, sizeof(valor)));
    TEST_ASSERT_EQUAL_INT(1, ehf_search(h, "chave1", &out, &out_size));
    TEST_ASSERT_EQUAL_INT(42, out);
    TEST_ASSERT_EQUAL_UINT(sizeof(int), out_size);
}

void test_insert_chave_duplicada_deve_falhar(void)
{
    int valor1 = 10;
    int valor2 = 20;

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "abc", &valor1, sizeof(valor1)));
    TEST_ASSERT_EQUAL_INT(0, ehf_insert(h, "abc", &valor2, sizeof(valor2)));
}

void test_exists_deve_indicar_presenca_da_chave(void)
{
    int valor = 7;

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(0, ehf_exists(h, "x"));
    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "x", &valor, sizeof(valor)));
    TEST_ASSERT_EQUAL_INT(1, ehf_exists(h, "x"));
}

void test_remove_deve_excluir_registro(void)
{
    int valor = 99;
    int out = 0;
    size_t out_size = sizeof(out);

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "rm", &valor, sizeof(valor)));
    TEST_ASSERT_EQUAL_INT(1, ehf_exists(h, "rm"));

    TEST_ASSERT_EQUAL_INT(1, ehf_remove(h, "rm"));
    TEST_ASSERT_EQUAL_INT(0, ehf_exists(h, "rm"));
    TEST_ASSERT_EQUAL_INT(0, ehf_search(h, "rm", &out, &out_size));
}

void test_open_deve_reabrir_arquivo_existente(void)
{
    int valor = 1234;
    int out = 0;
    size_t out_size = sizeof(out);

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "persist", &valor, sizeof(valor)));
    ehf_close(h);
    h = NULL;

    h = ehf_open(TEST_FILE);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_EQUAL_INT(1, ehf_search(h, "persist", &out, &out_size));
    TEST_ASSERT_EQUAL_INT(1234, out);
}

void test_open_arquivo_inexistente_deve_falhar(void)
{
    h = ehf_open("arquivo_que_nao_existe.dat");
    TEST_ASSERT_NULL(h);
}

void test_funcoes_com_argumentos_invalidos(void)
{
    int valor = 1;
    int out = 0;
    size_t out_size = sizeof(out);

    TEST_ASSERT_EQUAL_INT(-1, ehf_insert(NULL, "a", &valor, sizeof(valor)));
    TEST_ASSERT_EQUAL_INT(-1, ehf_insert(NULL, NULL, &valor, sizeof(valor)));
    TEST_ASSERT_EQUAL_INT(-1, ehf_search(NULL, "a", &out, &out_size));
    TEST_ASSERT_EQUAL_INT(-1, ehf_remove(NULL, "a"));
    TEST_ASSERT_EQUAL_INT(-1, ehf_exists(NULL, "a"));
    TEST_ASSERT_EQUAL_INT(-1, ehf_global_depth(NULL));
    TEST_ASSERT_EQUAL_INT(-1, ehf_bucket_count(NULL));
}

void test_global_depth_e_bucket_count_devem_ser_validos(void)
{
    int v1 = 1, v2 = 2, v3 = 3, v4 = 4;

    h = ehf_create(TEST_FILE, 2);
    TEST_ASSERT_NOT_NULL(h);

    TEST_ASSERT_TRUE(ehf_global_depth(h) >= 0);
    TEST_ASSERT_TRUE(ehf_bucket_count(h) >= 1);

    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "k1", &v1, sizeof(v1)));
    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "k2", &v2, sizeof(v2)));
    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "k3", &v3, sizeof(v3)));
    TEST_ASSERT_EQUAL_INT(1, ehf_insert(h, "k4", &v4, sizeof(v4)));

    TEST_ASSERT_TRUE(ehf_global_depth(h) >= 0);
    TEST_ASSERT_TRUE(ehf_bucket_count(h) >= 1);
}

int main(void)
{
    UNITY_BEGIN();

    RUN_TEST(test_create_deve_retornar_handle_valido);
    RUN_TEST(test_insert_e_search_devem_funcionar);
    RUN_TEST(test_insert_chave_duplicada_deve_falhar);
    RUN_TEST(test_exists_deve_indicar_presenca_da_chave);
    RUN_TEST(test_remove_deve_excluir_registro);
    RUN_TEST(test_open_deve_reabrir_arquivo_existente);
    RUN_TEST(test_open_arquivo_inexistente_deve_falhar);
    RUN_TEST(test_funcoes_com_argumentos_invalidos);
    RUN_TEST(test_global_depth_e_bucket_count_devem_ser_validos);

    return UNITY_END();
}